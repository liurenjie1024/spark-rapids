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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import io.glutenproject.execution._
import io.substrait.proto.Plan

import ai.rapids.cudf.{HostColumnVector, NvtxColor}
import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric, GpuSemaphore, NvtxWithMetrics}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.TrampolineUtil
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
      val hostIter: Iterator[Array[HostColumnVector]] = if (preloadedCapacity > 0) {
        val producerInitFn = () => {
          coalesceConverter.setRuntime()
        }
        PreloadedIterator(context.taskAttemptId(),
          coalesceConverter,
          producerInitFn,
          preloadedCapacity,
          metrics("preloadWaitTime"))
      } else {
        coalesceConverter.setRuntime()
        coalesceConverter
      }

      VeloxColumnarBatchConverter.hostToDevice(hostIter, outputAttr, metrics)
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

private case class PreloadedIterator(taskAttId: Long,
                                     iterImpl: Iterator[Array[HostColumnVector]],
                                     producerInitFn: () => Unit,
                                     capacity: Int,
                                     waitTimeMetric: GpuMetric
                                    ) extends Iterator[Array[HostColumnVector]] with Logging {

  @transient @volatile private var isInit: Boolean = false
  @transient @volatile private var isProducing: Boolean = false
  @transient @volatile private var readIndex: Int = 0
  @transient @volatile private var writeIndex: Int = 0
  // This lock guarantees anytime if ProducerStatus == running there must be a working batch
  // being produced or waiting to be put into the queue.
  @transient private lazy val hasNextLock = new ReentrantLock()

  @transient private lazy val emptyLock = new ReentrantLock()
  @transient private lazy val fullLock = new ReentrantLock()

  private var producer: Thread = _

  @transient private lazy val buffer: Array[Either[Throwable, Array[HostColumnVector]]] = {
    Array.ofDim[Either[Throwable, Array[HostColumnVector]]](capacity)
  }

  @transient private lazy val produceFn: Runnable = new Runnable {

    // This context will be got in the main Thread during the initialization of `produceFn`
    private val taskContext: TaskContext = TaskContext.get()

    override def run(): Unit = {
      TrampolineUtil.setTaskContext(taskContext)
      hasNextLock.lock()
      try {
        do {
          isProducing = true
          hasNextLock.unlock()

          do {
            fullLock.synchronized {
              if (writeIndex - readIndex == capacity) {
                fullLock.wait()
              }
            }
          } while (writeIndex - readIndex == capacity)

          buffer(writeIndex % capacity) = Right(iterImpl.next())
          emptyLock.synchronized {
            writeIndex += 1
            emptyLock.notify()
          }

          hasNextLock.lock()
          isProducing = false
          logError(s"[$taskAttId] PreloadedIterator produced $writeIndex batches, " +
            s"currently preloaded batchNum: ${writeIndex - readIndex}")
        }
        while (iterImpl.hasNext)
        hasNextLock.unlock()
      } catch {
        case ex: Throwable =>
          // transfer the exception info to the main thread as an interrupted signal
          buffer(writeIndex % capacity) = Left(ex)
          writeIndex += 1
          isProducing = false
          if (hasNextLock.isHeldByCurrentThread) {
            hasNextLock.unlock()
          }
          throw new RuntimeException(ex)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def hasNext: Boolean = {
    if (!isInit) {
      if (!iterImpl.hasNext) {
        return false
      }
      isInit = true
      isProducing = true
      producerInitFn()
      producer = new Thread(produceFn)
      producer.start()
      return true
    }

    writeIndex > readIndex || {
      hasNextLock.lock()
      val ret = writeIndex > readIndex || isProducing
      hasNextLock.unlock()
      ret
    }
  }

  override def next(): Array[HostColumnVector] = {
    if (writeIndex == readIndex) {
      GpuSemaphore.releaseIfNecessary(TaskContext.get())
      withResource(new NvtxWithMetrics("waitForCPU", NvtxColor.RED, waitTimeMetric)) { _ =>
        do {
          emptyLock.synchronized {
            if (writeIndex == readIndex) {
              emptyLock.wait()
            }
          }
        } while (writeIndex == readIndex)
      }
    }
    val ret = buffer(readIndex % capacity)
    fullLock.synchronized {
      readIndex += 1
      fullLock.notify()
    }
    ret match {
      case Left(ex: Throwable) =>
        logError(s"[$taskAttId] PreloadedIterator: AsyncProducer failed with exceptions")
        throw new RuntimeException(s"[$taskAttId] PreloadedIterator", ex)
      case Right(ret: Array[HostColumnVector]) =>
        logError(s"[$taskAttId] PreloadedIterator consumed $readIndex batches, " +
          s"currently preloaded batchNum: ${writeIndex - readIndex}")
        ret
    }
  }
}
