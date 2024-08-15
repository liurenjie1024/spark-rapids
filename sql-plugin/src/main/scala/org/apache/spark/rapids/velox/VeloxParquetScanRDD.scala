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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import io.glutenproject.execution._
import io.substrait.proto.Plan

import ai.rapids.cudf.{HostColumnVector, NvtxColor}
import com.nvidia.spark.rapids.{CoalesceSizeGoal, GpuMetric, NvtxWithMetrics}
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
                                     preloadedCapacity: Int,
                                     waitTimeMetric: GpuMetric
                                    ) extends Iterator[Array[HostColumnVector]] with Logging {

  private var consumedNum: Int = 0
  private var producedNum: Int = 0

  /**
   * Status code of the async Producer:
   * 0: uninitialized
   * 1: running
   * 2: finished
   */
  @transient private lazy val producerStatus = new AtomicInteger(0)

  // This lock guarantees anytime if ProducerStatus == running there must be a working batch
  // being produced or waiting to be put into the queue.
  @transient private lazy val lock = new ReentrantLock()

  private var producer: Thread = _

  @transient private lazy val preloadedBatches = {
    new LinkedBlockingQueue[Either[Throwable, Array[HostColumnVector]]](preloadedCapacity)
  }

  @transient private lazy val produceFn: Runnable = new Runnable {

    // This context will be got in the main Thread during the initialization of `produceFn`
    private val taskContext: TaskContext = TaskContext.get()

    override def run(): Unit = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        var firstTime = true

        do {
          if (firstTime) {
            firstTime = false
          } else {
            lock.unlock()
          }
          val nextBatch = iterImpl.next()
          lock.lock()
          producedNum += 1
          logInfo(s"[$taskAttId] PreloadedIterator produced $producedNum batches, currently " +
            s"preloaded batchNum: ${preloadedBatches.size() + 1}")
          preloadedBatches.put(Right(nextBatch))
        }
        while (iterImpl.hasNext)

        require(producerStatus.incrementAndGet() == 2)
        lock.unlock()
      } catch {
        case ex: Throwable =>
          // transfer the exception info to the main thread as an interrupted signal
          preloadedBatches.put(Left(ex))
          if (lock.isHeldByCurrentThread) {
            lock.unlock()
          }
          throw new RuntimeException(ex)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def hasNext: Boolean = {
    if (producerStatus.get() == 0) {
      if (!iterImpl.hasNext) {
        producerStatus.getAndSet(2)
        return false
      }
      producerInitFn()
      producerStatus.incrementAndGet()
      producer = new Thread(produceFn)
      producer.start()
    }

    !preloadedBatches.isEmpty || {
      lock.lock()
      val ret = producerStatus.get() == 1
      lock.unlock()
      ret
    }
  }

  override def next(): Array[HostColumnVector] = {
    var ret = preloadedBatches.poll()
    if (ret == null) {
      withResource(new NvtxWithMetrics("waitForCPU", NvtxColor.RED, waitTimeMetric)) { _ =>
        ret = preloadedBatches.take()
      }
    }
    ret match {
      case Left(ex: Throwable) =>
        logError(s"[$taskAttId] PreloadedIterator: AsyncProducer failed with exceptions")
        throw new RuntimeException(s"[$taskAttId] PreloadedIterator", ex)
      case Right(ret: Array[HostColumnVector]) =>
        consumedNum += 1
        logInfo(s"[$taskAttId] PreloadedIterator consumed $consumedNum batches")
        ret
    }
  }
}
