package com.nvidia.spark.rapids.shuffle

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.Arm.withResource

case class PartitionedHostColumnarBatch(
    numRows: Int,
    partitionOffsets: Array[Int],
    hostVectors: Array[HostColumnVector]) extends AutoCloseable {
  lazy val memorySize: Long = hostVectors.map(_.getHostMemorySize).sum

  def getPartition(partitionId: Int): HostColumnarBatchPartition = {
    val offset = partitionOffsets(partitionId)
    val numRows = if (partitionId == partitionOffsets.length - 1) {
      this.numRows - offset
    } else {
      partitionOffsets(partitionId + 1) - offset
    }
    HostColumnarBatchPartition(partitionId, offset, numRows, hostVectors)
  }

  override def close(): Unit = {
    withResource(hostVectors) { _ =>
    }
  }
}

case class HostColumnarBatchPartition(partitionId: Int, start: Int, numRows: Int,
    hostVectors: Array[HostColumnVector])
