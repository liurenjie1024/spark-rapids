/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.util

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable
import scala.reflect.ClassTag

import ai.rapids.cudf.{JCudfSerialization, NvtxColor, NvtxRange, Schema => CudfSchema}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableProducingSeq, AutoCloseableSeq}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitTargetSizeInHalfGpu, withRetry}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import com.nvidia.spark.rapids.shuffle.kudo.{HostMergeResult, SerializedTable}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Coalesces serialized tables on the host up to the target batch size before transferring
 * the coalesced result to the GPU. This reduces the overhead of copying data to the GPU
 * and also helps avoid holding onto the GPU semaphore while shuffle I/O is being performed.
 *
 * @note This should ALWAYS appear in the plan after a GPU shuffle when RAPIDS shuffle is
 *       not being used.
 */
case class GpuShuffleCoalesceExec(child: SparkPlan, targetBatchByteSize: Long)
  extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    CONCAT_MERGE_HEADER_TIME -> createNanoTimingMetric(DEBUG_LEVEL,
      DESCRIPTION_CONCAT_MERGE_HEADER_TIME),
    CONCAT_MERGE_HOST_BUFFER_TIME -> createNanoTimingMetric(DEBUG_LEVEL,
      DESCRIPTION_CONCAT_MERGE_HOST_BUFFER_TIME),
    NUM_SPLIT_RETRY -> createMetric(DEBUG_LEVEL, DESCRIPTION_SPLIT_RETRY)
  )

  override protected val outputBatchesLevel = MODERATE_LEVEL

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val metricsMap = allMetrics
    val targetSize = targetBatchByteSize
    val dataTypes = GpuColumnVector.extractTypes(schema)
    val rapidsConf = new RapidsConf(conf)
    val useSplitRetryRead = rapidsConf.shuffleSplitRetryReadEnabled
    val kudoOpt = rapidsConf.getKudoConf()

    child.executeColumnar().mapPartitions { iter =>
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
        iter, targetSize, dataTypes, useSplitRetryRead, metricsMap, kudoOpt)
    }
  }
}

object GpuShuffleCoalesceUtils {
  def getGpuShuffleCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      targetSize: Long,
      dataTypes: Array[DataType],
      useSplitRetryRead: Boolean,
      metricsMap: Map[String, GpuMetric],
      kudoConf: Option[KudoConf] = None,
      prefetchFirstBatch: Boolean = false): Iterator[ColumnarBatch] = {
    if (useSplitRetryRead) {
      val reader = kudoConf.map { kConf =>
        new KudoShuffleCoalesceReader(iter, targetSize, dataTypes, metricsMap, kConf)
      }.getOrElse(
        new GpuShuffleCoalesceReader(iter, targetSize, dataTypes, metricsMap))

      if (prefetchFirstBatch) {
        withResource(new NvtxRange("fetch first batch", NvtxColor.YELLOW)) { _ =>
          // Force a coalesce of the first batch before we grab the GPU semaphore
          reader.prefetchHeadOnHost()
        }
      }
      reader.asIterator
    } else {
      val hostIter = kudoConf.map { kConf =>
        new KudoShuffleCoalesceIterator(iter, targetSize, metricsMap, kConf, dataTypes)
      } getOrElse {
        new HostShuffleCoalesceIterator(iter, targetSize, metricsMap)
      }
      val maybeBufferedIter = if (prefetchFirstBatch) {
        val bufferedIter = new CloseableBufferedIterator(hostIter)
        withResource(new NvtxRange("fetch first batch", NvtxColor.YELLOW)) { _ =>
          // Force a coalesce of the first batch before we grab the GPU semaphore
          bufferedIter.headOption
        }
        bufferedIter
      } else {
        hostIter
      }
      new GpuShuffleCoalesceIterator(maybeBufferedIter, dataTypes, metricsMap)
    }
  }

  def toGpu(result: KudoMergeResult, dataTypes: Array[DataType],
      cudfSchema: Option[CudfSchema] = None): ColumnarBatch = {
    val schema = cudfSchema.getOrElse(GpuColumnVector.from(dataTypes))
    result.merged match {
      case Right(rows) => new ColumnarBatch(Array.empty, rows)
      case Left(hostMergeResult) =>
        withResource(hostMergeResult.toContiguousTable(schema)) { ct =>
          GpuColumnVectorFromBuffer.from(ct, dataTypes)
        }
    }
  }

  def toGpuIfAllowed(
      table: AnyRef,
      dataTypes: Array[DataType],
      cudfSchema: Option[CudfSchema] = None): ColumnarBatch = table match {
    case m: KudoMergeResult => toGpu(m, dataTypes, cudfSchema)
    case c: HostConcatResult =>
      cudf_utils.HostConcatResultUtil.getColumnarBatch(c, dataTypes)
    case o =>
      throw new IllegalArgumentException(s"unsupported type: ${o.getClass}")
  }

  def getHostShuffleCoalesceIterator(
      iter: BufferedIterator[ColumnarBatch],
      targetSize: Long,
      dataTypes: Array[DataType],
      coalesceMetrics: Map[String, GpuMetric],
      kudoConf: Option[KudoConf]): Option[Iterator[AutoCloseable]] = {
    var retIter: Option[Iterator[AutoCloseable]] = None
    if (iter.hasNext && iter.head.numCols() == 1) {
      iter.head.column(0) match {
        case _: SerializedTableColumn =>
          retIter = Some(new HostShuffleCoalesceIterator(iter, targetSize, coalesceMetrics))
        case _: KudoSerializedTableColumn =>
          retIter = Some(new KudoShuffleCoalesceIterator(iter, targetSize, coalesceMetrics,
            kudoConf.get, dataTypes))
        case _ => // should be gpu batches
      }
    }
    retIter
  }

  def getCoalescedBufferSize(concated: AnyRef): Long = concated match {
    case m: KudoMergeResult => m.getDataLen
    case c: HostConcatResult => c.getTableHeader.getDataLen
    case g => GpuColumnVector.getTotalDeviceMemoryUsed(g.asInstanceOf[ColumnarBatch])
  }

  def getSerializedBufferSize(cb: ColumnarBatch): Long = {
    assert(cb.numCols() == 1)
    val hmb = cb.column(0) match {
      case kudoCol: KudoSerializedTableColumn =>
        kudoCol.inner.getBuffer
      case serCol: SerializedTableColumn =>
        serCol.hostBuffer
      case o => throw new IllegalStateException(s"unsupported type: ${o.getClass}")
    }
    if (hmb != null) hmb.getLength else 0L
  }
}

case class KudoMergeResult(merged: Either[HostMergeResult, Int]) extends AutoCloseable {

  def getDataLen: Long = merged match {
    case Left(m) => m.getDataLen
    case _ => 0L
  }

  override def close(): Unit = merged match {
    case Left(ret) => ret.close()
    case _ =>
  }
}

sealed trait TableOperator[T <: AutoCloseable, C] {
  def getDataLen(table: T): Long
  def getNumRows(table: T): Int
  def concatOnHost(tables: Array[T]): C
  def toGpu(c: C, dataTypes: Array[DataType]): ColumnarBatch
}

class CudfTableOperator extends TableOperator[SerializedTableColumn, HostConcatResult] {
  override def getDataLen(table: SerializedTableColumn): Long = table.header.getDataLen
  override def getNumRows(table: SerializedTableColumn): Int = table.header.getNumRows

  override def concatOnHost(tables: Array[SerializedTableColumn]): HostConcatResult = {
    assert(tables.nonEmpty, "no tables to be concatenated")
    val numCols = tables.head.header.getNumColumns
    if (numCols == 0) {
      val totalRowsNum = tables.map(getNumRows).sum
      cudf_utils.HostConcatResultUtil.rowsOnlyHostConcatResult(totalRowsNum)
    } else {
      val (headers, buffers) = tables.map(t => (t.header, t.hostBuffer)).unzip
      JCudfSerialization.concatToHostBuffer(headers, buffers)
    }
  }

  override def toGpu(c: HostConcatResult, dataTypes: Array[DataType]): ColumnarBatch = {
    cudf_utils.HostConcatResultUtil.getColumnarBatch(c, dataTypes)
  }
}

class KudoTableOperator(
    dataTypes: Array[DataType],
    kudoConf: KudoConf,
    mergeHeaderMetric: GpuMetric,
    mergeHostBufferMetric: GpuMetric
) extends TableOperator[KudoSerializedTableColumn, KudoMergeResult] {

  private val kudoSer = kudoConf.serializer()
  private val cudfSchema = GpuColumnVector.from(dataTypes)

  override def getDataLen(table: KudoSerializedTableColumn): Long =
    table.inner.getHeader.getTotalDataLen
  override def getNumRows(table: KudoSerializedTableColumn): Int =
    table.inner.getHeader.getNumRows.toInt

  override def concatOnHost(tables: Array[KudoSerializedTableColumn]): KudoMergeResult = {
    assert(tables.nonEmpty, "no tables to be concatenated")
    val numCols = tables.head.inner.getHeader.getNumColumns
    val merged = if (numCols == 0) {
      Right(tables.map(getNumRows).sum)
    } else {
      val serTables = new util.ArrayList[SerializedTable](tables.length)
      tables.foreach(t => serTables.add(t.inner))

      val result = kudoSer.mergeToHost(serTables, cudfSchema)

      mergeHeaderMetric += result.getRight.getCalcHeaderTime
      mergeHostBufferMetric += result.getRight.getMergeIntoHostBufferTime

      Left(result.getLeft)
    }
    KudoMergeResult(merged)
  }

  override def toGpu(c: KudoMergeResult, dataTypes: Array[DataType]): ColumnarBatch = {
    GpuShuffleCoalesceUtils.toGpu(c, dataTypes, Some(cudfSchema))
  }
}

/**
 * Reader to coalesce columnar batches that are expected to contain only serialized
 * tables from shuffle. The serialized tables within are collected up to the target
 * batch size and then concatenated on the host. Next try to send the concatenated
 * result to GPU.
 * When OOM happens, it will reduce the target size by half, try to concatenate
 * half of cached tables and send the result to GPU again.
 */
abstract class GpuShuffleCoalesceReaderBase[T <: AutoCloseable: ClassTag, C](
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric]) extends AutoCloseable {
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
  private[this] val splitRetriesMetric = metricsMap(GpuMetric.NUM_SPLIT_RETRY)

  private[this] val serializedTables = new mutable.Queue[T]
  private[this] var realBatchSize = math.max(targetBatchSize, 1)
  private[this] var closed = false

  protected def tableOperator: TableOperator[T, C]

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  override def close(): Unit = if (!closed) {
    serializedTables.foreach(_.close())
    serializedTables.clear()
    closed = true
  }

  private def pullNextBatch(): Boolean = {
    if (closed) return false
    // Always make sure enough data has been cached for the next batch.
    var curCachedSize = serializedTables.map(tableOperator.getDataLen).sum
    var curCachedRows = serializedTables.map(tableOperator.getNumRows(_).toLong).sum
    while (iter.hasNext && curCachedSize < realBatchSize && curCachedRows < Int.MaxValue) {
      closeOnExcept(iter.next()) { batch =>
        inputBatchesMetric += 1
        inputRowsMetric += batch.numRows()
        if (batch.numRows > 0) {
          val tableCol = batch.column(0).asInstanceOf[T]
          serializedTables.enqueue(tableCol)
          curCachedSize += tableOperator.getDataLen(tableCol)
          curCachedRows += tableOperator.getNumRows(tableCol)
        } else {
          batch.close()
        }
      }
    }
    serializedTables.nonEmpty
  }

  private def collectTablesForNextBatch(targetSize: Long): Array[T] = {
    var curSize = 0L
    var curRows = 0L
    val taken = serializedTables.takeWhile { tableCol =>
      curSize += tableOperator.getDataLen(tableCol)
      curRows += tableOperator.getNumRows(tableCol)
      curSize <= targetSize && curRows < Int.MaxValue
    }
    if (taken.isEmpty) {
      // The first batch size is bigger than targetSize, always take it
      Array(serializedTables.head)
    } else {
      taken.toArray
    }
  }

  private val reduceBatchSizeByHalf: AutoCloseableTargetSize => Seq[AutoCloseableTargetSize] =
    batchSize => {
      val halfSize = splitTargetSizeInHalfGpu(batchSize)
      assert(halfSize.length == 1)
      // Remember the size for the following caching and collecting.
      println(s"=>Update target batch size from $realBatchSize to ${halfSize.head.targetSize}")
      realBatchSize = halfSize.head.targetSize
      splitRetriesMetric += 1
      halfSize
    }

  private def buildNextBatch(): ColumnarBatch = {
    val closeableBatchSize = AutoCloseableTargetSize(realBatchSize, 1)
    val iter = withRetry(closeableBatchSize, reduceBatchSizeByHalf) { attemptSize =>
      val (concatRet, numTables) = withResource(new MetricRange(opTimeMetric)) { _ =>
        // Retry steps:
        //   1) Collect tables from cache for the next batch according to the target size.
        //   2) Concatenate the collected tables
        //   3) Move the concatenated result to GPU
        // We have to re-collect the tables and re-concatenate them, because the
        // HostConcatResult can not be split into smaller pieces.
        val curTables = collectTablesForNextBatch(attemptSize.targetSize)
        val concatHostBatch = withResource(new MetricRange(concatTimeMetric)) { _ =>
          tableOperator.concatOnHost(curTables)
        }
        (concatHostBatch, curTables.length)
      }
      withResourceIfAllowed(concatRet) { _ =>
        // Begin to use GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withResource(new MetricRange(opTimeMetric)) { _ =>
          (tableOperator.toGpu(concatRet, dataTypes), numTables)
        }
      }
    }
    // Expect only one batch
    val (batch, numTables) = iter.next()
    closeOnExcept(batch) { _ =>
      assert(iter.isEmpty)
      // Now it is ok to remove the first numTables table from cache.
      (0 until numTables).safeMap(_ => serializedTables.dequeue()).safeClose()
      batch
    }
  }

  def prefetchHeadOnHost(): this.type = {
    if (serializedTables.isEmpty) {
      pullNextBatch()
    }
    this
  }

  def asIterator: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
    override def hasNext: Boolean = pullNextBatch()
    override def next(): ColumnarBatch = {
      if (!hasNext) {
        throw new NoSuchElementException("No more host batches to read")
      }
      val batch = buildNextBatch()
      outputBatchesMetric += 1
      outputRowsMetric += batch.numRows()
      batch
    }
  }
}

class GpuShuffleCoalesceReader(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric])
  extends GpuShuffleCoalesceReaderBase[SerializedTableColumn, HostConcatResult](iter,
    targetBatchSize, dataTypes, metricsMap) {

  override protected val tableOperator = new CudfTableOperator
}

class KudoShuffleCoalesceReader(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric],
    kudoConf: KudoConf)
  extends GpuShuffleCoalesceReaderBase[KudoSerializedTableColumn, KudoMergeResult](
    iter, targetBatchSize, dataTypes, metricsMap) {

  private[this] val mergeHeaderTimeMetric = metricsMap(GpuMetric.CONCAT_MERGE_HEADER_TIME)
  private[this] val mergeHostBufferTimeMetric = metricsMap(
    GpuMetric.CONCAT_MERGE_HOST_BUFFER_TIME)

  override protected val tableOperator = new KudoTableOperator(dataTypes, kudoConf,
    mergeHeaderTimeMetric, mergeHostBufferTimeMetric)
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * serialized tables from shuffle. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
abstract class HostCoalesceIteratorBase[T <: AutoCloseable: ClassTag, C](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric]) extends Iterator[C] with AutoCloseable {
  private[this] val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
  private[this] val serializedTables = new util.ArrayDeque[T]
  private[this] var numTablesInBatch: Int = 0
  private[this] var numRowsInBatch: Int = 0
  private[this] var batchByteSize: Long = 0L

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  protected def tableOperator: TableOperator[T, C]

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
  }

  private def concatenateTablesInHost(): C = {
    val result = withResource(new MetricRange(concatTimeMetric)) { _ =>
      withResource(new Array[T](numTablesInBatch)) { tables =>
        tables.indices.foreach(i => tables(i) = serializedTables.removeFirst())
        tableOperator.concatOnHost(tables)
      }
    }

    // update the stats for the next batch in progress
    numTablesInBatch = serializedTables.size
    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1,
        "should only track at most one buffer that is not in a batch")
      val firstTable = serializedTables.peekFirst()
      batchByteSize = tableOperator.getDataLen(firstTable)
      numRowsInBatch = tableOperator.getNumRows(firstTable)
    }
    result
  }

  private def bufferNextBatch(): Unit = {
    if (numTablesInBatch == serializedTables.size()) {
      var batchCanGrow = batchByteSize < targetBatchByteSize
      while (batchCanGrow && iter.hasNext) {
        closeOnExcept(iter.next()) { batch =>
          inputBatchesMetric += 1
          // don't bother tracking empty tables
          if (batch.numRows > 0) {
            inputRowsMetric += batch.numRows()
            val tableColumn = batch.column(0).asInstanceOf[T]
            batchCanGrow = canAddToBatch(tableColumn)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableOperator.getNumRows(tableColumn)
              batchByteSize += tableOperator.getDataLen(tableColumn)
            }
          } else {
            batch.close()
          }
        }
      }
    }
  }

  override def hasNext(): Boolean = {
    bufferNextBatch()
    numTablesInBatch > 0
  }

  override def next(): C = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }
    concatenateTablesInHost()
  }

  private def canAddToBatch(nextTable: T): Boolean = {
    if (batchByteSize + tableOperator.getDataLen(nextTable) > targetBatchByteSize) {
      return false
    }
    if (numRowsInBatch.toLong + tableOperator.getNumRows(nextTable) > Integer.MAX_VALUE) {
      return false
    }
    true
  }
}

class HostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric])
  extends HostCoalesceIteratorBase[SerializedTableColumn, HostConcatResult](iter,
    targetBatchByteSize, metricsMap) {
  override protected def tableOperator = new CudfTableOperator
}

class KudoShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric],
    kudoConf: KudoConf,
    dataTypes: Array[DataType])
  extends HostCoalesceIteratorBase[KudoSerializedTableColumn, KudoMergeResult](
    iter, targetBatchByteSize, metricsMap) {
  override protected def tableOperator = new KudoTableOperator(dataTypes, kudoConf,
    metricsMap(GpuMetric.CONCAT_MERGE_HEADER_TIME),
    metricsMap(GpuMetric.CONCAT_MERGE_HOST_BUFFER_TIME))
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * [[SerializedTableColumn]] or [[KudoSerializedTableColumn]]. The serialized
 * tables within are collected up to the target batch size and then concatenated
 * on the host before the data is transferred to the GPU.
 */
class GpuShuffleCoalesceIterator(iter: Iterator[AnyRef],
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric]) extends Iterator[ColumnarBatch] {
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
  private[this] val cudfSchema = GpuColumnVector.from(dataTypes)

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more columnar batches")
    }
    withResource(new NvtxRange("Concat+Load Batch", NvtxColor.YELLOW)) { _ =>
      val hostConcatResult = withResource(new MetricRange(opTimeMetric)) { _ =>
        // op time covers concat time performed in `iter.next()`.
        // Note the concat runs on CPU.
        // GPU time = opTime - concatTime
        iter.next()
      }

      withResourceIfAllowed(hostConcatResult) { _ =>
        // We acquire the GPU regardless of whether `hostConcatResult`
        // is an empty batch or not, because the downstream tasks expect
        // the `GpuShuffleCoalesceIterator` to acquire the semaphore and may
        // generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withResource(new MetricRange(opTimeMetric)) { _ =>
          val batch = GpuShuffleCoalesceUtils.toGpuIfAllowed(hostConcatResult,
            dataTypes, Some(cudfSchema))
          outputBatchesMetric += 1
          outputRowsMetric += batch.numRows()
          batch
        }
      }
    }
  }
}
