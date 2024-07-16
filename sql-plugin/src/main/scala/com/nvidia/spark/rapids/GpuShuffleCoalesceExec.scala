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

import scala.collection.convert.ImplicitConversions._

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.{HostConcatResult, SerializedTableHeader}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import com.nvidia.spark.rapids.shuffle.TableSerializer
import com.nvidia.spark.rapids.shuffle.kudo.SerializedTable
import java.util
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
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
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

    val kudoOpt = Kudo.getKudoConf(conf)

    child.executeColumnar().mapPartitions { iter =>
      if (kudoOpt.isDefined) {
        new KudoShuffleCoalesceIterator(iter, targetSize, metricsMap, kudoOpt.get.serializer(),
          dataTypes)
      } else {
        new GpuShuffleCoalesceIterator(
          new HostShuffleCoalesceIterator(iter, targetSize, metricsMap),
          dataTypes, metricsMap)
      }
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * [[SerializedTableColumn]]. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
class HostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric])
  extends Iterator[HostConcatResult] with AutoCloseable {
  private[this] val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
  private[this] val serializedTables = new util.ArrayDeque[SerializedTableColumn]
  private[this] var numTablesInBatch: Int = 0
  private[this] var numRowsInBatch: Int = 0
  private[this] var batchByteSize: Long = 0L

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
  }

  def concatenateTablesInHost(): HostConcatResult = {
    val result = withResource(new MetricRange(concatTimeMetric)) { _ =>
      val firstHeader = serializedTables.peekFirst().header
      if (firstHeader.getNumColumns == 0) {
        (0 until numTablesInBatch).foreach(_ => serializedTables.removeFirst())
        cudf_utils.HostConcatResultUtil.rowsOnlyHostConcatResult(numRowsInBatch)
      } else {
        val headers = new Array[SerializedTableHeader](numTablesInBatch)
        withResource(new Array[HostMemoryBuffer](numTablesInBatch)) { buffers =>
          headers.indices.foreach { i =>
            val serializedTable = serializedTables.removeFirst()
            headers(i) = serializedTable.header
            buffers(i) = serializedTable.hostBuffer
          }
          JCudfSerialization.concatToHostBuffer(headers, buffers)
        }
      }
    }

    // update the stats for the next batch in progress
    numTablesInBatch = serializedTables.size

    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1,
        "should only track at most one buffer that is not in a batch")
      val header = serializedTables.peekFirst().header
      batchByteSize = header.getDataLen
      numRowsInBatch = header.getNumRows
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
            val tableColumn = batch.column(0).asInstanceOf[SerializedTableColumn]
            batchCanGrow = canAddToBatch(tableColumn.header)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableColumn.header.getNumRows
              batchByteSize += tableColumn.header.getDataLen
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

  override def next(): HostConcatResult = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }
    concatenateTablesInHost()
  }

  private def canAddToBatch(nextTable: SerializedTableHeader): Boolean = {
    if (batchByteSize + nextTable.getDataLen > targetBatchByteSize) {
      return false
    }
    if (numRowsInBatch.toLong + nextTable.getNumRows > Integer.MAX_VALUE) {
      return false
    }
    true
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * [[SerializedTableColumn]]. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before the data
 * is transferred to the GPU.
 */
class GpuShuffleCoalesceIterator(iter: Iterator[HostConcatResult],
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric])
  extends Iterator[ColumnarBatch] {
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)

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

      withResource(hostConcatResult) { _ =>
        // We acquire the GPU regardless of whether `hostConcatResult`
        // is an empty batch or not, because the downstream tasks expect
        // the `GpuShuffleCoalesceIterator` to acquire the semaphore and may
        // generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        withResource(new MetricRange(opTimeMetric)) { _ =>
          val batch = cudf_utils.HostConcatResultUtil.getColumnarBatch(hostConcatResult, dataTypes)
          outputBatchesMetric += 1
          outputRowsMetric += batch.numRows()
          batch
        }
      }
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * [[KudoSerializer.SerializedTable]]. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
class KudoShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric],
    kudo: TableSerializer,
    dataTypes: Array[DataType])
  extends Iterator[ColumnarBatch] with AutoCloseable {
  private[this] val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
  private[this] val serializedTables = new util.ArrayDeque[SerializedTable]
  private[this] var numTablesInBatch: Int = 0
  private[this] var numRowsInBatch: Int = 0
  private[this] var batchByteSize: Long = 0L

  private val cudfSchema = GpuColumnVector.from(dataTypes)

//  {
//    System.err.println(s"Spark schema: ${dataTypes.mkString("Spark(", ", ", ")")}, cudf " +
//      s"schema: $cudfSchema")
//  }


  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
  }

  def concatenateTables(): ColumnarBatch = {
    val result = withResource(new MetricRange(concatTimeMetric)) { _ =>
      val firstHeader = serializedTables.peekFirst().getHeader
      if (firstHeader.getNumColumns() == 0) {
        (0 until numTablesInBatch).foreach(_ => serializedTables.removeFirst())
        new ColumnarBatch(Array.empty, numRowsInBatch)
      } else {
        val (left, right) = serializedTables.splitAt(numTablesInBatch)

        serializedTables.clear()

        serializedTables.addAll(right)

        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        withResource(left.toList) { serTables =>
          withResource(kudo.mergeTable(serTables, cudfSchema)) { cudfTable =>
            GpuColumnVectorFromBuffer.from(cudfTable, dataTypes)
          }
        }

      }
    }

    // update the stats for the next batch in progress
    numTablesInBatch = serializedTables.size

    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1,
        "should only track at most one buffer that is not in a batch")
      val header = serializedTables.peekFirst().getHeader
      batchByteSize = header.getTotalDataLen
      numRowsInBatch = header.getNumRows.toInt
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
            val tableColumn = batch.column(0).asInstanceOf[KudoSerializedTableColumn].inner
            batchCanGrow = canAddToBatch(tableColumn)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableColumn.getHeader.getNumRows.toInt
              batchByteSize += tableColumn.getHeader.getTotalDataLen
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

  override def next(): ColumnarBatch = {
    withResource(new NvtxRange("Concat+Load Batch", NvtxColor.YELLOW)) { _ =>
      withResource(new MetricRange(opTimeMetric)) { _ =>
        if (!hasNext()) {
          throw new NoSuchElementException("No more host batches to concatenate")
        }
        val batch = concatenateTables()

//        {
//          val taskContext = TaskContext.get()
//          if (taskContext != null) {
//            val name = s"shuffle_coalesce_result, stage_id=${taskContext.stageId()}, " +
//              s"partition_id=${taskContext.partitionId()}"
//            GpuColumnVector.debug(name, batch)
//          }
//        }
        outputBatchesMetric += 1
        outputRowsMetric += batch.numRows()
        batch
      }
    }
  }

  private def canAddToBatch(nextTable: SerializedTable): Boolean = {
    if (batchByteSize + nextTable.getHeader.getTotalDataLen > targetBatchByteSize) {
      return false
    }
    if (numRowsInBatch.toLong + nextTable.getHeader.getNumRows > Integer.MAX_VALUE) {
      return false
    }
    true
  }
}
