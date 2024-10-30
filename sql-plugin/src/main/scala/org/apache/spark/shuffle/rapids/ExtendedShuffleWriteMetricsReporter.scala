package org.apache.spark.shuffle.rapids

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor}
import org.apache.spark.shuffle.rapids.ExtendedShuffleWriteMetricsReporter.{ASYNC_COMPRESSION_TIME, ASYNC_SORT_TIME, ASYNC_WAIT_LIMIT_TIME, SYNC_COMPRESSION_TIME, SYNC_SORT_TIME, SYNC_WAIT_LIMIT_TIME}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter.{SHUFFLE_BYTES_WRITTEN, SHUFFLE_RECORDS_WRITTEN, SHUFFLE_WRITE_TIME}

class ExtendedShuffleWriteMetricsReporter(
    private val inner: ShuffleWriteMetricsReporter,
    private val metrics: Map[String, SQLMetric]
) extends ShuffleWriteMetricsReporter {

  private val bytesWritten = metrics(SHUFFLE_BYTES_WRITTEN)
  private val recordsWritten = metrics(SHUFFLE_RECORDS_WRITTEN)
  private val writeTime = metrics(SHUFFLE_WRITE_TIME)

  private val syncCompressionTime = metrics(SYNC_COMPRESSION_TIME)
  private val syncWaitLimitTime = metrics(SYNC_WAIT_LIMIT_TIME)
  private val syncSortTime = metrics(SYNC_SORT_TIME)
  private val asyncCompressionTime = metrics(ASYNC_COMPRESSION_TIME)
  private val asyncWaitLimitTime = metrics(ASYNC_WAIT_LIMIT_TIME)
  private val asyncSortTime = metrics(ASYNC_SORT_TIME)

  override private[spark] def incBytesWritten(v: Long): Unit = {
    inner.incBytesWritten(v)
    bytesWritten += v
  }

  override private[spark] def incRecordsWritten(v: Long): Unit = {
    inner.incRecordsWritten(v)
    recordsWritten += v
  }

  override private[spark] def incWriteTime(v: Long): Unit = {
    inner.incWriteTime(v)
    writeTime += v
  }

  override private[spark] def decBytesWritten(v: Long): Unit = {
    inner.decBytesWritten(v)
    bytesWritten += -v
  }

  override private[spark] def decRecordsWritten(v: Long): Unit = {
    inner.decRecordsWritten(v)
    recordsWritten += -v
  }

  def incSyncCompressionTime(v: Long): Unit = {
    syncCompressionTime += v
  }

  def incSyncWaitLimitTime(v: Long): Unit = {
    syncWaitLimitTime += v
  }

  def incSyncSortTime(v: Long): Unit = {
    syncSortTime += v
  }

  def incAsyncCompressionTime(v: Long): Unit = {
    asyncCompressionTime += v
  }

  def incAsyncWaitLimitTime(v: Long): Unit = {
    asyncWaitLimitTime += v
  }

  def incAsyncSortTime(v: Long): Unit = {
    asyncSortTime += v
  }
}

object ExtendedShuffleWriteMetricsReporter {
  val SYNC_COMPRESSION_TIME = "syncCompressionTime"
  val SYNC_WAIT_LIMIT_TIME = "syncWaitLimitTime"
  val SYNC_SORT_TIME = "syncSortTime"

  val ASYNC_COMPRESSION_TIME = "asyncCompressionTime"
  val ASYNC_WAIT_LIMIT_TIME = "asyncWaitLimitTime"
  val ASYNC_SORT_TIME = "asyncSortTime"

  def createMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    Map(
      SYNC_COMPRESSION_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Sync compression time"),
      SYNC_WAIT_LIMIT_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Sync wait limit time"),
      SYNC_SORT_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Sync sort time"),
      ASYNC_COMPRESSION_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Async compression time"),
      ASYNC_WAIT_LIMIT_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Async wait limit time"),
      ASYNC_SORT_TIME -> SQLMetrics.createNanoTimingMetric(sc, "Async sort time")
    )
  }

  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor() {
      override def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
        new ExtendedShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}