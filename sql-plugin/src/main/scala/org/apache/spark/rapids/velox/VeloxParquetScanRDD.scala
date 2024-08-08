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

import io.glutenproject.execution._
import io.substrait.proto.Plan

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class VeloxParquetScanRDD(scanRDD: RDD[ColumnarBatch],
                          outputAttr: Seq[Attribute],
                          outputSchema: StructType,
                          coalesceGoal: CoalesceSizeGoal,
                          useNativeConverter: Boolean,
                          metrics: Map[String, GpuMetric])
  extends RDD[InternalRow](scanRDD.sparkContext, Nil) {

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
    val veloxCbIter = new VeloxScanMetricsIter(
      scanRDD.compute(split, context),
      veloxScanTime
    )
    val deviceIter = if (useNativeConverter) {
      VeloxColumnarBatchConverter.nativeConvert(
        veloxCbIter, outputAttr, coalesceGoal, metrics)
    } else {
      VeloxColumnarBatchConverter.roundTripConvert(
        veloxCbIter, outputAttr, coalesceGoal, metrics)
    }

    // TODO: SPARK-25083 remove the type erasure hack in data source scan
    new InterruptibleIterator(context, deviceIter.asInstanceOf[Iterator[InternalRow]])
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
