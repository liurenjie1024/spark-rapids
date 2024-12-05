package com.nvidia.spark.rapids.shuffle

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVectorBase

import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


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

  def toColumnarBatch: ColumnarBatch = {
    new ColumnarBatch(Array[ColumnVector](PartitionedHostColumnarBatchColumn(this)), numRows)
  }
}

case class PartitionedHostColumnarBatchColumn(inner: PartitionedHostColumnarBatch) extends
  GpuColumnVectorBase(NullType) {

  override def close(): Unit = inner.close()

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")
}



case class HostColumnarBatchPartition(partitionId: Int, start: Int, numRows: Int,
    hostVectors: Array[HostColumnVector])
