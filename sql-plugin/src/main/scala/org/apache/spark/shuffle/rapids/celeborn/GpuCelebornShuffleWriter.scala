package org.apache.spark.shuffle.rapids.celeborn

import java.io.IOException
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import scala.reflect.ClassTag

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.{OpenByteArrayOutputStream, SendBufferPool, SortBasedPusher, SparkUtils}
import org.apache.spark.shuffle.rapids.celeborn.GpuCelebornShuffleWriter.{DEFAULT_INITIAL_SER_BUFFER_SIZE, OBJECT_CLASS_TAG}
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.unsafe.Platform


class GpuCelebornShuffleWriter[K, V](
    val dep: GpuShuffleDependency[K, V, V],
    val numMappers: Int,
    val taskContext: TaskContext,
    val conf: CelebornConf,
    val shuffleClient: ShuffleClient,
    val metrics: ShuffleWriteMetricsReporter,
    val sendBufferPool: SendBufferPool,
) extends ShuffleWriter[K, V] with Logging {

  private val serBuffer = new OpenByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE)
  private val serOutputStream = dep.serializer.newInstance().serializeStream(serBuffer)

  private val mapId = taskContext.partitionId()
  private val numPartitions = dep.partitioner.numPartitions
  private val mapStatusLengths = Array.fill(numPartitions)(new LongAdder())
  private val pusher = new SortBasedPusher(taskContext.taskMemoryManager,
    shuffleClient, taskContext, dep.shuffleId, mapId,
    taskContext.attemptNumber,
    taskContext.taskAttemptId,
    numMappers, numPartitions, conf,
    v => metrics.incBytesWritten(v.longValue()),
    mapStatusLengths,
    conf.clientPushSortMemoryThreshold,
    sendBufferPool)

  private val pushBufferMaxSize = conf.clientPushBufferMaxSize

  private var tmpRecordsWritten = 0L
  private var peakMemoryUsedBytes = 0L
  private val stopping: AtomicBoolean = new AtomicBoolean(false)


  override def write(records: Iterator[Product2[K, V]]): Unit = {
    doWrite(records)
    close()
  }


  private def doWrite(records: Iterator[Product2[K, V]]): Unit = {
    for ((k, v) <- records) {
     val partitionId = dep.partitioner.getPartition(k)
      serBuffer.reset();
      serOutputStream.writeKey(k, OBJECT_CLASS_TAG)
      serOutputStream.writeValue(v, OBJECT_CLASS_TAG)
      serOutputStream.flush()

      val serializedRecordSize = serBuffer.size()

      if (serializedRecordSize > pushBufferMaxSize) {
        pushGiantRecord(partitionId, serBuffer.getBuf, serializedRecordSize)
      } else {
        var success = pusher.insertRecord(serBuffer.getBuf, Platform.BYTE_ARRAY_OFFSET,
          serializedRecordSize, partitionId, false)

        if (!success) {
          doPush()
          success = pusher.insertRecord(serBuffer.getBuf, Platform.BYTE_ARRAY_OFFSET,
            serializedRecordSize, partitionId, false)

          if (!success) {
            throw new IOException("Unable to push after switching pusher!")
          }
        }
      }

      tmpRecordsWritten += 1
    }
  }

  private def pushGiantRecord(partitionId: Int, buffer: Array[Byte], numBytes: Int): Unit = {
    logDebug(s"Pushing giant record of size $numBytes to partition $partitionId")
    val bytesWritten = shuffleClient.pushData(dep.shuffleId, mapId, taskContext.attemptNumber(),
      partitionId,
      buffer,
      0,
      numBytes,
      numMappers,
      numPartitions)

    mapStatusLengths(partitionId).add(bytesWritten)
    metrics.incRecordsWritten(bytesWritten)
  }

  private def doPush(): Unit = {
    val start = System.nanoTime()
    pusher.pushData(true)
    metrics.incWriteTime(System.nanoTime() - start)
  }

  private def close(): Unit = {
    logInfo(s"Closing writer for mapId $mapId, memory used ${pusher.getUsed}")

    val start = System.nanoTime()
    pusher.pushData(false)
    pusher.close()

    shuffleClient.pushMergedData(dep.shuffleId, mapId, taskContext.attemptNumber())
    metrics.incWriteTime(System.nanoTime() - start)
    metrics.incRecordsWritten(tmpRecordsWritten)

    val waitStartTime = System.nanoTime()
    shuffleClient.mapperEnd(dep.shuffleId, mapId, taskContext.attemptNumber(), numMappers)
    metrics.incWriteTime(System.nanoTime() - waitStartTime)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsed)
      if (!stopping.get()) {
        stopping.set(true)
        if (success) {
          val bmId = SparkEnv.get.blockManager.shuffleServerId
          val mapStatus = SparkUtils.createMapStatus(bmId,
            SparkUtils.unwrap(mapStatusLengths), taskContext.taskAttemptId())

          if (mapStatus != null) {
            Some(mapStatus)
          } else {
            throw new IllegalStateException("Cannot call stop(true) without having called write()")
          }
        } else {
          None
        }
      } else {
        None
      }
    } finally {
      shuffleClient.cleanup(dep.shuffleId, mapId, taskContext.attemptNumber())
    }
  }

  override def getPartitionLengths(): Array[Long] = throw new UnsupportedOperationException(
    "Celeborn is not compatible with push-based shuffle, " +
      "please set spark.shuffle.push.enabled to false")

  private def updatePeakMemoryUsed(): Unit = {
    val memoryUsed = pusher.getUsed
    if (memoryUsed > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = memoryUsed
    }
  }

  def getPeakMemoryUsed: Long = {
    updatePeakMemoryUsed()
    peakMemoryUsedBytes
  }
}

object GpuCelebornShuffleWriter {
  private val DEFAULT_INITIAL_SER_BUFFER_SIZE: Int = 1024 * 1024
  private val OBJECT_CLASS_TAG: ClassTag[AnyRef]  = ClassTag[AnyRef]
}
