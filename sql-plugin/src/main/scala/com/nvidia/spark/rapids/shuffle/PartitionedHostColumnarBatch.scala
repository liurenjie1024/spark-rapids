package com.nvidia.spark.rapids.shuffle

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.Arm.withResource

case class PartitionedHostColumnarBatch(
    partitionIds: Array[Int],
    partitionOffsets: Array[Int],
    hostVectors: Array[HostColumnVector]) extends AutoCloseable {
  lazy val memorySize: Long = hostVectors.map(_.getHostMemorySize).sum

  override def close(): Unit = {
    withResource(hostVectors) { _ =>

    }
  }
}

case class HostColumnarBatchPartition(partitionId: Int, start: Int, offset: Int,
    hostVectors: Array[HostColumnVector])
