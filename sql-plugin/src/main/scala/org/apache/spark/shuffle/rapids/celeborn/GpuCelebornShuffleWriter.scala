package org.apache.spark.shuffle.rapids.celeborn

import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuExec.createNanoTimingMetric
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.shuffle.{HostColumnarBatchPartition, PartitionedHostColumnarBatch, PartitionedHostColumnarBatchColumn}
import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.write.DataPusher
import org.apache.celeborn.common.CelebornConf

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.celeborn.{OpenByteArrayOutputStream, SendBufferPool, SparkUtils, TaskInterruptedHelper}
import org.apache.spark.shuffle.rapids.celeborn.GpuCelebornShuffleWriter.{DEFAULT_INITIAL_SER_BUFFER_SIZE, METRIC_CLOSE_TIME, METRIC_DO_PUSH_TIME, METRIC_DO_WRITE_TIME, METRIC_STOP_TIME}
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuCelebornShuffleWriter[K, V](
    val dep: GpuShuffleDependency[K, V, V],
    val numMappers: Int,
    val taskContext: TaskContext,
    val conf: CelebornConf,
    val shuffleClient: ShuffleClient,
    val metricsReporter: ShuffleWriteMetricsReporter,
    val sendBufferPool: SendBufferPool,
) extends ShuffleWriter[K, V] with Logging {

  private val mapId = taskContext.partitionId()
  private val numPartitions = dep.partitioner.numPartitions
  private val mapStatusLengths = Array.fill(numPartitions)(new LongAdder())
  private val gpuPusher = {
    val dataPusher = new DataPusher(dep.shuffleId,
      mapId,
      taskContext.attemptNumber(),
      taskContext.taskAttemptId(),
      numMappers,
      numPartitions,
      conf,
      shuffleClient,
      sendBufferPool.acquirePushTaskQueue(),
      x => metricsReporter.incBytesWritten(x.toLong),
      mapStatusLengths)
    new GpuDataPusher(conf.clientPushSortMemoryThreshold,
      dep.serializer.newInstance(),
      sendBufferPool,
      dataPusher,
      dep.metrics,
      numPartitions)
  }

  private val stopping: AtomicBoolean = new AtomicBoolean(false)

  private val extraMetrics = dep.metrics
  private val doWriteTime: GpuMetric = extraMetrics(METRIC_DO_WRITE_TIME)
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
      doWriteTime.ns {
        val batch = r._2.asInstanceOf[ColumnarBatch]
        require(batch.numCols() == 1, "Celeborn shuffle only supports one column")
        batch.column(0) match {
          case PartitionedHostColumnarBatchColumn(partedTable) =>
            gpuPusher.insert(partedTable)
          case _ =>
            throw new IllegalStateException("Unsupported column type " + batch.column(0).getClass)
        }
      }
    }
  }

  private def close(): Unit = {
    logInfo(s"Closing writer for mapId $mapId")

    gpuPusher.close()

    shuffleClient.pushMergedData(dep.shuffleId, mapId, taskContext.attemptNumber())
    metricsReporter.incRecordsWritten(gpuPusher.recordsWritten)

    shuffleClient.mapperEnd(dep.shuffleId, mapId, taskContext.attemptNumber(), numMappers)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    GpuMetric.ns(stopTime) {
      try {
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
}

object GpuCelebornShuffleWriter {
  private[celeborn] val DEFAULT_INITIAL_SER_BUFFER_SIZE: Int = 1024 * 1024

  private[celeborn] val METRIC_DO_PUSH_TIME = "celeborn.doPushTime"
  private[celeborn] val METRIC_DO_WRITE_TIME = "celeborn.doWriteTime"
  private[celeborn] val METRIC_CLOSE_TIME = "celeborn.closeTime"
  private[celeborn] val METRIC_STOP_TIME = "celeborn.stopTime"
  private[celeborn] val METRIC_COPY_TO_SHUFFLE_CLIENT_TIME = "celeborn.copyToShuffleClientTime"
  private[celeborn] val METRIC_OFFER_DATA_TIME = "celeborn.offerDataTime"

  def createMetrics(sc: SparkContext): Map[String, GpuMetric] = {
    Map(
      METRIC_DO_WRITE_TIME -> createNanoTimingMetric(sc, "celeborn do write time"),
      METRIC_DO_PUSH_TIME -> createNanoTimingMetric(sc, "celeborn do push time"),
      METRIC_CLOSE_TIME -> createNanoTimingMetric(sc, "celeborn close time"),
      METRIC_STOP_TIME -> createNanoTimingMetric(sc, "celeborn stop time"),
      METRIC_COPY_TO_SHUFFLE_CLIENT_TIME -> createNanoTimingMetric(sc,
        "celeborn copy to shuffle client time"),
      METRIC_OFFER_DATA_TIME -> createNanoTimingMetric(sc, "celeborn offer data time")
    )
  }
}

class GpuDataPusher(val maxBufferSize: Long,
    val serializerInst: SerializerInstance,
    val sendBufferPool: SendBufferPool,
    val dataPusher: DataPusher,
    val metrics: Map[String, GpuMetric],
    val numPartitions: Int,
) {
  private val memoryBuf = new ArrayBuffer[PartitionedHostColumnarBatch](32)
  private var accumulatedBufferSize = 0L
  private val serBuffer = new OpenByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE)
  private var tmpRecordsWritten: Long = 0L


  private val doPushTime: GpuMetric = metrics(METRIC_DO_PUSH_TIME)

  def insert(partitionedTable: PartitionedHostColumnarBatch) = {
    memoryBuf += partitionedTable
    accumulatedBufferSize += partitionedTable.memorySize
    if (accumulatedBufferSize >= maxBufferSize) {
      doPush()
    }
  }

  def close(): Unit = {
    pushLeft
    try {
      dataPusher.waitOnTermination()
      sendBufferPool.returnPushTaskQueue(dataPusher.getIdleQueue)
    } catch {
      case _: InterruptedException =>
        TaskInterruptedHelper.throwTaskKillException()
    }
  }

  private def pushLeft = {
    if (memoryBuf.nonEmpty) {
      doPush()
    }

  }

  def recordsWritten: Long = tmpRecordsWritten

  private def doPush(): Unit = {
    withResource(memoryBuf) { _ =>
      for (partitionId <- 0 until numPartitions) {
        pushOnePartition(partitionId, memoryBuf.map(_.getPartition(partitionId)))

        if (serBuffer.size() > 0) {
          doPushTime.ns {
            dataPusher.addTask(partitionId, serBuffer.getBuf, 0)
          }
        }
      }
    }

    accumulatedBufferSize = 0
    memoryBuf.clear()
  }

  private def pushOnePartition(partitionId: Int,
      partitions: Iterable[HostColumnarBatchPartition]) = {
    serBuffer.reset()
    val serStream = serializerInst.serializeStream(serBuffer)
    for (partition <- partitions) {
      if (partition.numRows > 0) {
        serStream.writeKey(partitionId)
        serStream.writeValue(partition)

        tmpRecordsWritten += 1L
      }
    }
    serStream.flush()
  }
}