/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.velox

import scala.collection.mutable

import io.glutenproject.execution._
import io.substrait.proto.Plan

import ai.rapids.cudf.{HostColumnVector, NvtxColor}
import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric, GpuSemaphore, NvtxWithMetrics, SemaphoreAcquired}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class VeloxParquetScanRDD(scanRDD: RDD[ColumnarBatch],
                          outputAttr: Seq[Attribute],
                          outputSchema: StructType,
                          coalesceGoal: CoalesceSizeGoal,
                          metrics: Map[String, GpuMetric],
                          useNativeConverter: Boolean,
                          preloadedCapacity: Int
                         ) extends RDD[InternalRow](scanRDD.sparkContext, Nil) {

  private val veloxScanTime = GpuMetric.unwrap(metrics("VeloxScanTime"))

  override protected def getPartitions: Array[Partition] = scanRDD.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    split match {
      case FirstZippedPartitionsPartition(_, inputPartition, _) => {
        inputPartition match {
          case GlutenPartition(_, plan, _, _) => {
            try {
              val planObj = Plan.parseFrom(plan)
              logInfo("Velox Parquet Scan Plan object: \n" + planObj)
            } catch {
              case _: Throwable => ()
            }
          }
          case GlutenRawPartition(_, _, splitInfos, _) => {
            try {
              splitInfos.foreach { splitInfo =>
                val filePartition = splitInfo.getFilePartition()
                filePartition match {
                  case FilePartition(_, files) => {
                    files.foreach { file =>
                      logWarning("Read parquet file with Velox: " + file)
                    }
                  }
                  case _ => {}
                }
              }
            } catch {
              case _: Throwable => ()
            }
          }
          case _ => { }
        }
      }
      case _ => {}
    }
    // the wrapping Iterator for the underlying VeloxScan task
    val veloxIter = new VeloxScanMetricsIter(scanRDD.compute(split, context), veloxScanTime)

    val resIter = if (!useNativeConverter) {
      VeloxColumnarBatchConverter.roundTripConvert(
        veloxIter, outputAttr, coalesceGoal, metrics)
    } else {
      // Preloading only works for NativeConverter because using roundTripConverter we
      // can NOT split the building process of HostColumnVector and the host2device process,
      // since they are completed in one by GpuRowToColumnConverter.

      val schema = StructType(outputAttr.map { ar =>
        StructField(ar.name, ar.dataType, ar.nullable)
      })
      require(coalesceGoal.targetSizeBytes <= Int.MaxValue,
        s"targetSizeBytes should be smaller than 2GB, but got ${coalesceGoal.targetSizeBytes}"
      )
      val coalesceConverter = new CoalesceNativeConverter(
        veloxIter, coalesceGoal.targetSizeBytes.toInt, schema, metrics
      )

      if (preloadedCapacity > 0) {
        val preloadIter = PreloadedIterator(coalesceConverter,
          preloadedCapacity,
          metrics("preloadWaitTime"), metrics("GpuAcquireTime")
        )
        VeloxColumnarBatchConverter.hostToDevice(preloadIter,
          outputAttr,
          metrics,
          acquireGpuSemaphore = false
        )
      } else {
        VeloxColumnarBatchConverter.hostToDevice(coalesceConverter,
          outputAttr,
          metrics,
          acquireGpuSemaphore = true
        )
      }
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, resIter.asInstanceOf[Iterator[InternalRow]])
  }

}

private class VeloxScanMetricsIter(iter: Iterator[ColumnarBatch],
                                   scanTime: SQLMetric
                                  ) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = {
    val start = System.nanoTime()
    try {
      iter.hasNext
    } finally {
      scanTime += System.nanoTime() - start
    }
  }

  override def next(): ColumnarBatch = {
    val start = System.nanoTime()
    try {
      iter.next()
    } finally {
      scanTime += System.nanoTime() - start
    }
  }
}

private case class PreloadedIterator(iterImpl: Iterator[Array[HostColumnVector]],
                                     preloadedCapacity: Int,
                                     waitCpuTime: GpuMetric,
                                     waitGpuTime: GpuMetric
                                    ) extends Iterator[Array[HostColumnVector]] with Logging {

  private var producedNum: Int = 0
  private var consumedNum: Int = 0
  private var gpuOccupied: Boolean = false
  private var hasWorkingBuffer: Boolean = false

  @transient private lazy val taskId = TaskContext.get().taskAttemptId()

  @transient private lazy val preloadedBatches = mutable.Queue.empty[Array[HostColumnVector]]

  private def progressLog(produced: Boolean, consumed: Boolean): Unit = {
    if (produced) {
      producedNum += 1
    }
    if (consumed) {
      consumedNum += 1
    }
    logInfo(s"[$taskId] PreloadedIterator produced $producedNum batches and" +
      s" consumed $consumedNum batches")
  }

  override def hasNext: Boolean = {
    preloadedBatches.nonEmpty || iterImpl.hasNext
  }

  private def popOne(): Array[HostColumnVector] = {
    if (preloadedBatches.nonEmpty) {
      progressLog(produced = false, consumed = true)
      return preloadedBatches.dequeue()
    }
    withResource(new NvtxWithMetrics("waitCpuScan", NvtxColor.RED, waitCpuTime)) { _ =>
      hasWorkingBuffer = false
      progressLog(produced = true, consumed = true)
      iterImpl.next()
    }
  }

  override def next(): Array[HostColumnVector] = {
    hasWorkingBuffer = true

    if (gpuOccupied) {
      return popOne()
    }

    while (preloadedBatches.size < preloadedCapacity && (hasWorkingBuffer || iterImpl.hasNext)) {
      if (preloadedBatches.nonEmpty) {
        GpuSemaphore.tryAcquire(TaskContext.get()) match {
          case SemaphoreAcquired => gpuOccupied = true
          case _ =>
        }
      }
      if (gpuOccupied) {
        return popOne()
      }
      hasWorkingBuffer = false
      progressLog(produced = true, consumed = false)
      preloadedBatches.enqueue(iterImpl.next())
    }

    withResource(new NvtxWithMetrics("gpuAcquireC2C", NvtxColor.GREEN, waitGpuTime)) { _ =>
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      gpuOccupied = true
    }
    progressLog(produced = false, consumed = true)
    preloadedBatches.dequeue()
  }
}
