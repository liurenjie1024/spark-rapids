package org.apache.spark.shuffle.rapids.celeborn

import java.io.IOException
import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import com.nvidia.spark.rapids.GpuMetric
import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.spark.{SparkContext, SparkEnv, TaskContext}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.{OpenByteArrayOutputStream, SendBufferPool, SortBasedPusher, SparkUtils}
import org.apache.spark.shuffle.rapids.celeborn.GpuCelebornShuffleWriter.{DEFAULT_INITIAL_SER_BUFFER_SIZE, METRIC_ACCU_BUFFER_TIME, METRIC_CLOSE_TIME, METRIC_DO_PUSH_TIME, METRIC_DO_SER_TIME, METRIC_PUSH_GIANT_RECORD_TIME, METRIC_STOP_TIME}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createNanoTimingMetric
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.Platform


class GpuCelebornShuffleWriter[K, V](
    val dep: GpuShuffleDependency[K, V, V],
    val numMappers: Int,
    val taskContext: TaskContext,
    val conf: CelebornConf,
    val shuffleClient: ShuffleClient,
    val metricsReporter: ShuffleWriteMetricsReporter,
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
    v => metricsReporter.incBytesWritten(v.longValue()),
    mapStatusLengths,
    conf.clientPushSortMemoryThreshold,
    sendBufferPool)

  private val pushBufferMaxSize = conf.clientPushBufferMaxSize

  private var tmpRecordsWritten = 0L
  private var peakMemoryUsedBytes = 0L
  private val stopping: AtomicBoolean = new AtomicBoolean(false)

  private val extraMetrics = GpuMetric.wrap(dep.metrics)
  private val accuBufferTime: GpuMetric = extraMetrics(METRIC_ACCU_BUFFER_TIME)
  private val doPushTime: GpuMetric = extraMetrics(METRIC_DO_PUSH_TIME)
  private val pushGiantRecordTime: GpuMetric = extraMetrics(METRIC_PUSH_GIANT_RECORD_TIME)
  private val doSerTime: GpuMetric = extraMetrics(METRIC_DO_SER_TIME)
  private val closeTime: GpuMetric = extraMetrics(METRIC_CLOSE_TIME)
  private val stopTime: GpuMetric = extraMetrics(METRIC_STOP_TIME)



  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val start = System.nanoTime()
    doWrite(records)
    GpuMetric.ns(closeTime) {
      close()
    }
    metricsReporter.incWriteTime(System.nanoTime() - start)
  }


  private def doWrite(records: Iterator[Product2[K, V]]): Unit = {
    for (r <- records) {
      val partitionId = r._1.asInstanceOf[Int]
      val batch = r._2.asInstanceOf[ColumnarBatch]
      GpuMetric.ns(doSerTime) {
        serBuffer.reset()
        serOutputStream.writeKey(partitionId)
        serOutputStream.writeValue(batch)
        serOutputStream.flush()
      }


      val serializedRecordSize = serBuffer.size()

      if (serializedRecordSize > pushBufferMaxSize) {
        GpuMetric.ns(pushGiantRecordTime) {
          pushGiantRecord(partitionId, serBuffer.getBuf, serializedRecordSize)
        }
      } else {
        var success = GpuMetric.ns(accuBufferTime) {
          pusher.insertRecord(serBuffer.getBuf,
            Platform.BYTE_ARRAY_OFFSET,
            serializedRecordSize, partitionId, false)
        }
        if (!success) {
          doPush()
          success = GpuMetric.ns(accuBufferTime) {
            pusher.insertRecord(serBuffer.getBuf,
              Platform.BYTE_ARRAY_OFFSET,
              serializedRecordSize, partitionId, false)
          }

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
    metricsReporter.incRecordsWritten(bytesWritten)
  }

  private def doPush(): Unit = {
    GpuMetric.ns(doPushTime) {
      pusher.pushData(true)
    }
  }

  private def close(): Unit = {
    logInfo(s"Closing writer for mapId $mapId, memory used ${pusher.getUsed}")

    pusher.pushData(false)
    pusher.close()

    shuffleClient.pushMergedData(dep.shuffleId, mapId, taskContext.attemptNumber())
    metricsReporter.incRecordsWritten(tmpRecordsWritten)

    shuffleClient.mapperEnd(dep.shuffleId, mapId, taskContext.attemptNumber(), numMappers)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    GpuMetric.ns(stopTime) {
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
              throw new IllegalStateException(
                "Cannot call stop(true) without having called write()")
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

  private val METRIC_ACCU_BUFFER_TIME = "accumulateBufferTime"
  private val METRIC_DO_PUSH_TIME = "celeborn.doPushTime"
  private val METRIC_DO_SER_TIME = "celeborn.doSerTime"
  private val METRIC_CLOSE_TIME = "celeborn.closeTime"
  private val METRIC_STOP_TIME = "celeborn.stopTime"
  private val METRIC_PUSH_GIANT_RECORD_TIME = "celeborn.pushGiantRecordTime"

  def createMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      METRIC_ACCU_BUFFER_TIME -> createNanoTimingMetric(sc,
        "celeborn accumulating buffer time "),
      METRIC_PUSH_GIANT_RECORD_TIME -> createNanoTimingMetric(sc,
        "celeborn push giant record time"),
      METRIC_DO_SER_TIME -> createNanoTimingMetric(sc, "celeborn do serialization time"),
      METRIC_DO_PUSH_TIME -> createNanoTimingMetric(sc, "celeborn do push time"),
      METRIC_CLOSE_TIME -> createNanoTimingMetric(sc, "celeborn close time"),
      METRIC_STOP_TIME -> createNanoTimingMetric(sc, "celeborn stop time")
    )
  }
}
