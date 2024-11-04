/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rapids.celeborn

import java.io.IOException
import java.util
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.{CelebornInputStream, MetricsCallback}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.{CelebornIOException, PartitionUnRetryAbleException}
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{MessageType, PartitionLocation, PbOpenStreamList, PbOpenStreamListResponse, PbStreamHandler}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{ExceptionMaker, JavaUtils, ThreadUtils, Utils}

import org.apache.spark.{InterruptibleIterator, ShuffleDependency, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.celeborn.{ExecutorShuffleIdTracker, SparkUtils}
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader.streamCreatorPool
import org.apache.spark.util.CompletionIterator

class GpuCelebornShuffleReader[K, C](
    handle: GpuCelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Int = 0,
    endMapIndex: Int = Int.MaxValue,
    context: TaskContext,
    conf: CelebornConf,
    metrics: ShuffleReadMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  require(dep.keyOrdering.isEmpty, "GpuCelebornShuffleReader does not support key ordering")
  require(dep.aggregator.isEmpty, "GpuCelebornShuffleReader does not support aggregation")

  private val shuffleClient = ShuffleClient.get(
    handle.appUniqueId,
    handle.lifecycleManagerHost,
    handle.lifecycleManagerPort,
    conf,
    handle.userIdentifier,
    handle.extension)

  private val exceptionRef = new AtomicReference[IOException]
  private val throwsFetchFailure = handle.throwsFetchFailure

  override def read(): Iterator[Product2[K, C]] = {

    val serializerInstance = newSerializerInstance(dep)

    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, context, false)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    logDebug(s"get shuffleId $shuffleId for appShuffleId ${handle.shuffleId} " +
      s"attemptNum ${context.stageAttemptNumber()}")

    // Update the context task metrics for each record read.
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {
        metrics.incRemoteBytesRead(bytesWritten)
        metrics.incRemoteBlocksFetched(1)
      }

      override def incReadTime(time: Long): Unit =
        metrics.incFetchWaitTime(time)
    }

    if (streamCreatorPool == null) {
      GpuCelebornShuffleReader.synchronized {
        if (streamCreatorPool == null) {
          streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool(
            "celeborn-create-stream-thread",
            conf.readStreamCreatorPoolThreads,
            60)
        }
      }
    }

    val exceptionMaker = new ExceptionMaker() {
      override def makeFetchFailureException(
          appShuffleId: Int,
          shuffleId: Int,
          partitionId: Int,
          e: Exception): Exception = {
        new FetchFailedException(
          null,
          appShuffleId,
          -1,
          -1,
          partitionId,
          SparkUtils.FETCH_FAILURE_ERROR_MSG + appShuffleId + "/" + shuffleId,
          e)
      }
    }

    val startTime = System.currentTimeMillis()
    val fetchTimeoutMs = conf.clientFetchTimeoutMs
    val localFetchEnabled = conf.enableReadLocalShuffleFile
    val localHostAddress = Utils.localHostName(conf)
    val shuffleKey = Utils.makeShuffleKey(handle.appUniqueId, shuffleId)
    // startPartition is irrelevant
    val fileGroups = shuffleClient.updateFileGroup(shuffleId, startPartition)
    // host-port -> (TransportClient, PartitionLocation Array, PbOpenStreamList)
    val workerRequestMap = new util.HashMap[
      String,
      (TransportClient, util.ArrayList[PartitionLocation], PbOpenStreamList.Builder)]()

    var partCnt = 0

    (startPartition until endPartition).foreach { partitionId =>
      if (fileGroups.partitionGroups.containsKey(partitionId)) {
        fileGroups.partitionGroups.get(partitionId).asScala.foreach { location =>
          partCnt += 1
          val hostPort = location.hostAndFetchPort
          if (!workerRequestMap.containsKey(hostPort)) {
            val client = shuffleClient.getDataClientFactory().createClient(
              location.getHost,
              location.getFetchPort)
            val pbOpenStreamList = PbOpenStreamList.newBuilder()
            pbOpenStreamList.setShuffleKey(shuffleKey)
            workerRequestMap.put(
              hostPort,
              (client, new util.ArrayList[PartitionLocation], pbOpenStreamList))
          }
          val (_, locArr, pbOpenStreamListBuilder) = workerRequestMap.get(hostPort)

          locArr.add(location)
          pbOpenStreamListBuilder.addFileName(location.getFileName)
            .addStartIndex(startMapIndex)
            .addEndIndex(endMapIndex)
          pbOpenStreamListBuilder.addReadLocalShuffle(
            localFetchEnabled && location.getHost.equals(localHostAddress))
        }
      }
    }

    val locationStreamHandlerMap: ConcurrentHashMap[PartitionLocation, PbStreamHandler] =
      JavaUtils.newConcurrentHashMap()

    val futures = workerRequestMap.values().asScala.map { entry =>
      streamCreatorPool.submit(new Runnable {
        override def run(): Unit = {
          val (client, locArr, pbOpenStreamListBuilder) = entry
          val msg = new TransportMessage(
            MessageType.BATCH_OPEN_STREAM,
            pbOpenStreamListBuilder.build().toByteArray)
          val pbOpenStreamListResponse =
            try {
              val response = client.sendRpcSync(msg.toByteBuffer, fetchTimeoutMs)
              TransportMessage.fromByteBuffer(response).getParsedPayload[PbOpenStreamListResponse]
            } catch {
              case _: Exception => null
            }
          if (pbOpenStreamListResponse != null) {
            0 until locArr.size() foreach { idx =>
              val streamHandlerOpt = pbOpenStreamListResponse.getStreamHandlerOptList.get(idx)
              if (streamHandlerOpt.getStatus == StatusCode.SUCCESS.getValue) {
                locationStreamHandlerMap.put(locArr.get(idx), streamHandlerOpt.getStreamHandler)
              }
            }
          }
        }
      })
    }.toList
    // wait for all futures to complete
    futures.foreach(f => f.get())
    val end = System.currentTimeMillis()
    logInfo(s"BatchOpenStream for $partCnt cost ${end - startTime}ms")

    def createInputStream(partitionId: Int): CelebornInputStream = {
      val locations =
        if (fileGroups.partitionGroups.containsKey(partitionId)) {
          new util.ArrayList(fileGroups.partitionGroups.get(partitionId))
        } else new util.ArrayList[PartitionLocation]()
      val streamHandlers =
        if (locations != null) {
          val streamHandlerArr = new util.ArrayList[PbStreamHandler](locations.size())
          locations.asScala.foreach { loc =>
            streamHandlerArr.add(locationStreamHandlerMap.get(loc))
          }
          streamHandlerArr
        } else null
      if (exceptionRef.get() == null) {
        try {
          shuffleClient.readPartition(
            shuffleId,
            handle.shuffleId,
            partitionId,
            context.attemptNumber(),
            startMapIndex,
            endMapIndex,
            if (throwsFetchFailure) exceptionMaker else null,
            locations,
            streamHandlers,
            fileGroups.mapAttempts,
            metricsCallback)
        } catch {
          case e: IOException =>
            logError(s"Exception caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, e)
            null
          case e: Throwable =>
            logError(s"Non IOException caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, new CelebornIOException(e))
            null
        }
      } else null
    }

    val recordIter = (startPartition until endPartition).iterator.map(partitionId => {
      if (handle.numMappers > 0) {
        val startFetchWait = System.nanoTime()
        val inputStream: CelebornInputStream = createInputStream(partitionId)
        if (exceptionRef.get() != null) {
          exceptionRef.get() match {
            case ce @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
              if (throwsFetchFailure &&
                shuffleClient.reportShuffleFetchFailure(handle.shuffleId, shuffleId)) {
                throw new FetchFailedException(
                  null,
                  handle.shuffleId,
                  -1,
                  -1,
                  partitionId,
                  SparkUtils.FETCH_FAILURE_ERROR_MSG + handle.shuffleId + "/" + shuffleId,
                  ce)
              } else {
                throw ce
              }
            case e => throw e
          }
        }
        metricsCallback.incReadTime(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait))
        // ensure inputStream is closed when task completes
        context.addTaskCompletionListener[Unit](_ => inputStream.close())
        (partitionId, inputStream)
      } else {
        (partitionId, CelebornInputStream.empty())
      }
    }).map { case (partitionId, inputStream) =>
      (partitionId, serializerInstance.deserializeStream(inputStream).asKeyValueIterator)
    }.flatMap { case (partitionId, iter) =>
      try {
        iter
      } catch {
        case e @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
          if (throwsFetchFailure &&
            shuffleClient.reportShuffleFetchFailure(handle.shuffleId, shuffleId)) {
            throw new FetchFailedException(
              null,
              handle.shuffleId,
              -1,
              -1,
              partitionId,
              SparkUtils.FETCH_FAILURE_ERROR_MSG + handle.shuffleId + "/" + shuffleId,
              e)
          } else {
            throw e
          }
      }
    }

    val iterWithUpdatedRecordsRead =
      recordIter.map { record =>
        metrics.incRecordsRead(1)
        record
      }

    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      iterWithUpdatedRecordsRead,
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[(Any, Any)](context, metricIter)
      .asInstanceOf[Iterator[(K, C)]]
  }

  def newSerializerInstance(dep: ShuffleDependency[K, _, C]): SerializerInstance = {
    dep.serializer.newInstance()
  }

}

object GpuCelebornShuffleReader {
  var streamCreatorPool: ThreadPoolExecutor = null
}
