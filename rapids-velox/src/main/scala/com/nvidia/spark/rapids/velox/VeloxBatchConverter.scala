/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

package com.nvidia.spark.rapids.velox

import scala.collection.mutable

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer, PinnedMemoryPool}
import ai.rapids.cudf.DType.DTypeEnum
import io.glutenproject.columnarbatch.IndicatorVector
import io.glutenproject.rapids.GlutenJniWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch


case class SampleColumnInfo(posInSchema: Int,
                            veloxType: VeloxDataTypes.Type,
                            readType: StructField,
                            numRows: Int,
                            offsetsSize: Int,
                            dataSize: Int,
                            children: Seq[SampleColumnInfo])

case class RapidsHostColumn(vector: HostColumnVector, usePinnedMemory: Boolean, totalBytes: Long)

private[velox] case class HostBufferInfo(buffer: HostMemoryBuffer, isPinned: Boolean)

case class VectorBuilder(rootBufferInfo: Option[HostBufferInfo],
                         posInSchema: Int,
                         field: StructField,
                         nullBufOffset: Option[Long],
                         dataBufOffset: Option[Long],
                         offsetBufOffset: Option[Long],
                         children: Seq[VectorBuilder]) {

  def build(tailInfo: Array[Long]): RapidsHostColumn = {
    require(rootBufferInfo.nonEmpty, "build should be only triggered on top-level columns")

    val rootBuf = rootBufferInfo.get
    try {
      val (vector, actualTotalBytes) = buildImpl(tailInfo, rootBuf.buffer)
      RapidsHostColumn(vector.asInstanceOf[HostColumnVector], rootBuf.isPinned, actualTotalBytes)
    } finally {
      // Close the shared root buffer after all child buffers have been built
      rootBuf.buffer.close()
    }
  }

  private def buildImpl(tailInfo: Array[Long],
                        sharedBuffer: HostMemoryBuffer): (HostColumnVectorCore, Long) = {

    var finalTotalBytes = 0L
    var childVecs = new java.util.ArrayList[HostColumnVectorCore]()
    children.foreach { b =>
      val (childVec, childTotalSize)  = b.buildImpl(tailInfo, sharedBuffer)
      childVecs.add(childVec)
      finalTotalBytes += childTotalSize
    }

    val dType = VeloxBatchConverter.mapSparkTypeToDType(field.dataType)
    val rowCount = tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 2))
    val nullCount = java.util.Optional.of(java.lang.Long.valueOf(
      tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 3))))

    val dataBuffer = dataBufOffset.map { offset =>
      val finalLength = tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 0))
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }
    val offsetBuffer = offsetBufOffset.map { offset =>
      val finalLength = tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 1))
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }
    val nullBuffer = nullBufOffset.map { offset =>
      val finalLength = VeloxBatchConverter.sizeOfNullMask(rowCount.toInt)
      finalTotalBytes += finalLength
      sharedBuffer.slice(offset, finalLength)
    }

    // Cast Map[child0, child1] => List[Struct[child0, child1]]
    if (field.dataType.isInstanceOf[MapType]) {
      val structCol = new HostColumnVectorCore(DType.STRUCT,
        childVecs.get(0).getRowCount, java.util.Optional.of(0L),
        null, null, null, childVecs)
      childVecs = new java.util.ArrayList[HostColumnVectorCore]()
      childVecs.add(structCol)
    }

    val vector: HostColumnVectorCore = if (rootBufferInfo.nonEmpty) {
      new HostColumnVector(dType, rowCount, nullCount,
        dataBuffer.orNull, nullBuffer.orNull, offsetBuffer.orNull,
        childVecs)
    } else {
      new HostColumnVectorCore(dType, rowCount, nullCount,
        dataBuffer.orNull, nullBuffer.orNull, offsetBuffer.orNull,
        childVecs)
    }
    (vector, finalTotalBytes)
  }
}

object VectorBuilder {
  val TAIL_INFO_OFFSET = 2
  val TAIL_INFO_STRIDE = 4

  // The schema of tail information of vectors which are ready to be flushed
  // 1. the tail position of dataBuffer
  // 2. the tail position of offsetBuffer
  // 3. total row count
  // 4. total null count
  private[rapids] def getTailInfoPos(colIndex: Int, infoIndex: Int): Int = {
    colIndex * TAIL_INFO_STRIDE + TAIL_INFO_OFFSET + infoIndex
  }


  /**
   *  1. ArrayLength
   *  2. pre-convert time (in mircoSecond)
   *  2. vector decode time (in mircoSecond)
   */
  val FIELD_METRIC_HEADER_SIZE = 3

  /**
   *  The schema of field metrics from NativeConverter
   *
   *  1. convert time (in mircoSecond)
   *  2. number of output batches
   *  3. number of output rows
   *  4. number of output size (in Bytes)
   *  5. number of null records
   *  6. number of unique records
   *  7. number of constant batches
   *  8. number of identity batches
   *  9. number of shuffle batches
   *  10. number of array-range batches
   */
  val FIELD_METRIC_STRIDE = 10

  private[rapids] def dumpFieldMetrics(metrics: Array[Long], fieldOffset: Int): String = {
    val timeMs = metrics(fieldOffset) / 1000 // Millisecond
    val batches = metrics(fieldOffset + 1)
    val rows = metrics(fieldOffset + 2)
    val bytes = metrics(fieldOffset + 3) / 1024 // KB
    val numNulls = metrics(fieldOffset + 4)
    val rowsWithDict = metrics(fieldOffset + 5)
    val constBatches = metrics(fieldOffset + 6)
    val identityBatches = metrics(fieldOffset + 7)
    val rangeBatches = metrics(fieldOffset + 8)
    val dictBatches = metrics(fieldOffset + 9)

    s" ${timeMs}ms ${rows}rows ${bytes}KB ${batches}batches(C:$constBatches|I:$identityBatches|" +
      s"R:$rangeBatches|S:$dictBatches) ${numNulls}nullRows ${rowsWithDict}rowsWithDict"
  }
}

class VeloxBatchConverter(runtime: GlutenJniWrapper,
                          nativeHandle: Long,
                          schema: StructType,
                          targetBatchSize: Int,
                          metrics: Map[String, SQLMetric]) extends Logging {

  private val columnBuilders = mutable.ArrayBuffer[VectorBuilder]()

  private var deckFilled = true

  private var eclipsed: Long = 0L

  // initialize the coalesce converter, creating TargetBuffers for the first target batch
  {
    resetTargetBuffers()
  }

  def isDeckFilled: Boolean = deckFilled

  def hasProceedingBuilders: Boolean = columnBuilders.nonEmpty

  def eclipsedNanoSecond: Long = eclipsed

  def close(): String = {
    require(!deckFilled, "The deck is NOT empty")
    require(columnBuilders.isEmpty, "Please flush existing ColumnBuilders at first")
    // We will get the final metrics for each column (and subColumn) before cleaning up.
    // Then, we will beautify and dump the metrics for performance inspection
    val nativeMetrics = runtime.closeCoalesceConverter(nativeHandle)
    VeloxBatchConverter.dumpMetrics(nativeMetrics, schema)
  }

  def tryAppendBatch(cb: ColumnarBatch): Boolean = {
    require(!deckFilled, "The deck is NOT empty")
    val start: Long = System.nanoTime()

    val handle = VeloxBatchConverter.getNativeBatchHandle(cb)
    val ret = if (runtime.appendBatch(nativeHandle, handle)) {
      true
    } else {
      deckFilled = true
      false
    }

    eclipsed += System.nanoTime() - start
    ret
  }

  def resetTargetBuffers(): Unit = {
    require(columnBuilders.isEmpty, "Please flush existing ColumnBuilders at first")
    require(deckFilled, "There is NO sample batch which should be on the deck")
    val start: Long = System.nanoTime()

    // Collect the size distribution of buffers from the sample batch
    val sampleInfo = runtime.encodeSampleInfo(nativeHandle)
    // Estimate the capacity of targetBatchSize in the number of source batch
    val estimatedBatchNum: Double = targetBatchSize.toDouble / sampleInfo(1)
    // Decode the sample distribution
    val decodedSampleInfo = VeloxBatchConverter.decodeSampleInfo(sampleInfo, schema)

    // Create ColumnBuilders while encoding the bufferPtrs
    val bufferPtrs = mutable.ArrayBuffer[Long]()
    bufferPtrs.append(0L)
    decodedSampleInfo.foreach { rootInfo =>
      columnBuilders += createVectorBuilder(
        bufferPtrs,
        estimatedBatchNum,
        rootInfo
      )
    }
    bufferPtrs(0) = bufferPtrs.length

    // reset the native reference of target Buffers.
    // NOTE: The method will consume the sample batch on the deck. So, the caller can
    // run `tryAppendBatch` right after this method.
    runtime.resetTargetRef(nativeHandle, bufferPtrs.toArray)
    deckFilled = false

    eclipsed += System.nanoTime() - start
  }

  /**
   * Truncate buffer according to the tail address got from native converter. And group up these
   * buffers as HostColumnVectors with the rowCount and nullCount also got from the native side.
   */
  def flushAndConvert(): Array[RapidsHostColumn] = {
    require(columnBuilders.nonEmpty, "ColumnBuilders has NOT been setup")
    val start: Long = System.nanoTime()

    val tailInfo = runtime.flush(nativeHandle)
    metrics("C2COutputSize") += tailInfo(1)
    VeloxBatchConverter.nativeMetaPrettyPrint(
      "TailInfoFlush", tailInfo, VectorBuilder.TAIL_INFO_OFFSET, VectorBuilder.TAIL_INFO_STRIDE)
    val ret = columnBuilders.map(_.build(tailInfo))
    columnBuilders.clear()

    eclipsed += System.nanoTime() - start
    ret.toArray
  }

  // Instead of allocating memory for each buffer, allocating a united memory buffer which can be
  // shared by all buffers (including buffers for nested children). With this approach, we can
  // easily distinguish if a (top-level) field based on PinnedMemory or PageableMemory. And setup
  // different metrics specialized for PinnedMemory_H2D and PageableMemory_H2D.
  private def createVectorBuilder(bufferPtrs: mutable.ArrayBuffer[Long],
                                  estimatedBatchNum: Double,
                                  rootInfo: SampleColumnInfo): VectorBuilder = {

    def impl(localOffset: Long, info: SampleColumnInfo): (VectorBuilder, Long) = {
      require(VeloxDataTypes.canConvert(info.veloxType, info.readType.dataType),
        s"can NOT convert ${info.veloxType} to ${info.readType.dataType}")

      var offset = localOffset
      // The schema within each vector: [type, dataBuffer, nullBuffer, offsetBuffer]
      // Firstly, push the TypeIndex
      bufferPtrs += VeloxDataTypes.encodeSparkType(info.readType.dataType).toLong
      // Then, the offset and length of dataBuffer
      val dataOffset: Option[Long] = if (info.dataSize > 0) {
        val estDataSize = (info.dataSize * estimatedBatchNum).toLong
        bufferPtrs.append(offset, estDataSize)
        offset += estDataSize
        Some(offset - estDataSize)
      } else {
        bufferPtrs.append(-1L, 0L)
        None
      }
      // After that, the offset and length of nullBuffer
      val nullOffset: Option[Long] = if (info.readType.nullable) {
        val estimatedRows = (info.numRows * estimatedBatchNum).toInt
        val estNullMaskBytes = VeloxBatchConverter.sizeOfNullMask(estimatedRows).toLong
        bufferPtrs.append(offset, estNullMaskBytes)
        offset += estNullMaskBytes
        Some(offset - estNullMaskBytes)
      } else {
        bufferPtrs.append(-1L, 0L)
        None
      }
      // Finally, the offset and length of offsetBuffer
      val offsetOffset: Option[Long] = if (info.offsetsSize > 0) {
        val estOffsetSize = (info.offsetsSize * estimatedBatchNum).toLong
        bufferPtrs.append(offset, estOffsetSize)
        offset += estOffsetSize
        Some(offset - estOffsetSize)
      } else {
        bufferPtrs.append(-1L, 0L)
        None
      }

      // bufferPtrs is in pre-order
      val childBuilders = info.children.map { ch =>
        val (builder, newOffset) = impl(offset, ch)
        offset = newOffset
        builder
      }

      (VectorBuilder(None, info.posInSchema, info.readType,
        nullOffset, dataOffset, offsetOffset,
        childBuilders), offset)
    }

    // Record the start point of bufferPtrs
    val bufferPtrsStart = bufferPtrs.length
    // Create non-root builders while computing the total size
    val (tmpRootBuilder, totalBytes) = impl(0, rootInfo)

    // Try allocate from PinnedMemoryPool. Fallback to PageableMemory if failed.
    val bufferInfo = PinnedMemoryPool.tryAllocate(totalBytes) match {
      case buf if buf == null =>
        HostBufferInfo(HostMemoryBuffer.allocate(totalBytes, false), isPinned = false)
      case buf =>
        HostBufferInfo(buf, isPinned = true)
    }

    // Replace local offsets with absolute memory address
    val addr = bufferInfo.buffer.getAddress
    (bufferPtrsStart until bufferPtrs.length by 7).foreach { i =>
      // dataAddr
      bufferPtrs(i + 1) = bufferPtrs(i + 1) match {
        case -1L => 0L
        case localOffset => localOffset + addr
      }
      // nullAddr
      bufferPtrs(i + 3) = bufferPtrs(i + 3) match {
        case -1L => 0L
        case localOffset => localOffset + addr
      }
      // offsetAddr
      bufferPtrs(i + 5) = bufferPtrs(i + 5) match {
        case -1L => 0L
        case localOffset => localOffset + addr
      }
    }

    VectorBuilder(Some(bufferInfo),
      tmpRootBuilder.posInSchema,
      tmpRootBuilder.field,
      tmpRootBuilder.nullBufOffset,
      tmpRootBuilder.dataBufOffset,
      tmpRootBuilder.offsetBufOffset,
      tmpRootBuilder.children
    )
  }
}

object VeloxBatchConverter extends Logging {

  def apply(runtime: GlutenJniWrapper,
            firstBatch: ColumnarBatch,
            targetBatchSize: Int,
            schema: StructType,
            metrics: Map[String, SQLMetric]): VeloxBatchConverter = {
    val nullableInfo = VeloxBatchConverter.encodeNullableInfo(schema)
    logDebug(s"nullableInfo: ${nullableInfo.mkString(" | ")}")
    val firstHandle = getNativeBatchHandle(firstBatch)
    val handle = runtime.buildCoalesceConverter(firstHandle, nullableInfo)
    new VeloxBatchConverter(runtime, handle, schema, targetBatchSize, metrics)
  }

  private def getNativeBatchHandle(cb: ColumnarBatch): Long = {
    cb.column(0) match {
      case indicator: IndicatorVector =>
        indicator.handle()
      case cv =>
        throw new IllegalArgumentException(
          s"Expecting IndicatorVector, but got ${cv.getClass}")
    }
  }

  private def dumpMetrics(metrics: Array[Long], schema: StructType): String = {
    val alignment = "****"
    val builder = mutable.StringBuilder.newBuilder
    builder.append(s"pre-convert time ${metrics(1) / 1000}ms\n")
      .append(s"char count time ${metrics(2) / 1000}ms\n")
    var offset = VectorBuilder.FIELD_METRIC_HEADER_SIZE

    val stack = mutable.Stack[(StructField, Int)]()
    schema.fields.reverseIterator.foreach(f => stack.push(f -> 1))

    while (stack.nonEmpty) {
      val (f, depth) = stack.pop()

      (1 to depth).foreach(_ => builder.append(alignment))
      builder
        .append(' ')
        .append(f.toString())
        .append(VectorBuilder.dumpFieldMetrics(metrics, offset))
        .append('\n')
      offset += VectorBuilder.FIELD_METRIC_STRIDE

      f.dataType match {
        case at: ArrayType =>
          val elem = StructField(f.name + "_elem", at.elementType, at.containsNull)
          stack.push(elem -> (depth + 1))
        case mt: MapType =>
          val valF = StructField(f.name + "_val", mt.valueType, mt.valueContainsNull)
          stack.push(valF -> (depth + 1))
          val keyF = StructField(f.name + "_key", mt.keyType, nullable = false)
          stack.push(keyF -> (depth + 1))
        case st: StructType =>
          st.fields.reverseIterator.foreach(ch => stack.push(ch -> (depth + 1)))
        case _ =>
      }
    }

    builder.toString()
  }

  private def decodeSampleInfo(meta: Array[Long],
                               schema: StructType): Array[SampleColumnInfo] = {
    case class DecodeHelper(
      var progress: Int,
      head: Int,
      bound: Int,
      parent: DecodeHelper,
      targetType: StructField,
      children: mutable.Queue[SampleColumnInfo] = mutable.Queue[SampleColumnInfo]()
    )

    val tupleSize = 5
    val headLength = 2
    val vectorSize = (meta.length - headLength) / tupleSize
    // nativeMetaPrettyPrint("decodeSampleInfo", meta, headLength, tupleSize)

    val buildAllocInfo = (helper: DecodeHelper, children: Seq[SampleColumnInfo]) => {
      val offset = helper.head * tupleSize + headLength

      SampleColumnInfo(
        posInSchema = helper.head,
        veloxType = VeloxDataTypes.decodeVeloxType(meta(offset + 1).toInt),
        readType = helper.targetType,
        numRows = meta(offset + 2).toInt,
        offsetsSize = meta(offset + 3).toInt,
        dataSize = meta(offset + 4).toInt,
        children = children)
    }

    val stack = mutable.Stack[DecodeHelper]()
    val virtualRoot = DecodeHelper(0, -1, vectorSize, null,
      StructField("virtualRoot", schema, nullable = false)
    )
    stack.push(virtualRoot)

    while (stack.nonEmpty) {
      val cursor = stack.top
      assert(cursor.progress <= cursor.bound)
      if (cursor.progress == cursor.bound) {
        stack.pop()
        if (cursor.parent != null) {
          assert(cursor.parent.progress < cursor.bound)
          cursor.parent.progress = cursor.bound
          cursor.parent.children.enqueue(buildAllocInfo(cursor, cursor.children))
        }
      } else {
        val children = mutable.ArrayBuffer[DecodeHelper]()
        val childFields = mutable.Queue[StructField]()
        cursor.targetType.dataType match {
          case ArrayType(et, hasNull) =>
            childFields.enqueue(StructField("", et, hasNull))
          case MapType(kt, vt, hasNull) =>
            childFields.enqueue(StructField("", kt, nullable = false))
            childFields.enqueue(StructField("", vt, hasNull))
          case StructType(f) =>
            childFields.enqueue(f: _*)
        }
        var i = cursor.progress
        while (i < cursor.bound) {
          val rangeEnd = meta(i * tupleSize + headLength).toInt
          children += DecodeHelper(i + 1, i, rangeEnd, cursor, childFields.dequeue())
          i = rangeEnd
        }
        // Reverse the childIterator to ensure children being handled in the original order.
        // Otherwise, the update of progress will NOT work.
        children.reverseIterator.foreach(stack.push)
      }
    }

    virtualRoot.children.toArray
  }

  private def nativeMetaPrettyPrint(title: String,
                                    array: Array[Long], offset: Int, step: Int): Unit = {
    lazy val message: String = {
      val sb = mutable.StringBuilder.newBuilder
      sb.append(title).append('\n')
      sb.append("==HEAD== ").append((0 until offset).map(array).mkString(" | ")).append('\n')
      (offset until array.length by step).foreach { i =>
        sb.append(s"  (${(i - offset) / step + 1}) ")
        sb.append((i until i + step).map(array).mkString(" | "))
        sb.append('\n')
      }
      sb.toString()
    }
    logDebug(message)
  }

  // ColumnView.getValidityBufferSize
  def sizeOfNullMask(rowNum: Int): Int = {
    val actualBytes = (rowNum + 7) >> 3
    ((actualBytes + 63) >> 6) << 6
  }

  def mapSparkTypeToDType(dt: DataType): DType = dt match {
    case _: BooleanType => DType.BOOL8
    case _: ByteType => DType.INT8
    case _: ShortType => DType.INT16
    case _: IntegerType => DType.INT32
    case _: LongType => DType.INT64
    case _: FloatType => DType.FLOAT32
    case _: DoubleType => DType.FLOAT64
    case _: StringType => DType.STRING
    case _: DateType => DType.TIMESTAMP_DAYS
    case _: ArrayType => DType.LIST
    case _: MapType => DType.LIST
    case _: StructType => DType.STRUCT
    case d: DecimalType if DecimalType.is32BitDecimalType(d) =>
      DType.create(DTypeEnum.DECIMAL32, -d.scale)
    case d: DecimalType if DecimalType.is64BitDecimalType(d) =>
      DType.create(DTypeEnum.DECIMAL64, -d.scale)
    case d: DecimalType =>
      DType.create(DTypeEnum.DECIMAL128, -d.scale)
    case dt => throw new IllegalArgumentException(s"unexpected $dt")
  }

  private def encodeNullableInfo(root: StructType): Array[Int] = {
    val flattened = mutable.ArrayBuffer.empty[Int]
    val stack = mutable.Stack[StructField]()
    root.reverseIterator.foreach(stack.push)
    while (stack.nonEmpty) {
      val field = stack.pop()
      flattened += (if (field.nullable) 1 else 0)
      field.dataType match {
        case at: ArrayType =>
          stack.push(StructField("ArrayElem", at.elementType, nullable = at.containsNull))
        case mt: MapType =>
          stack.push(StructField("MapValue", mt.valueType, nullable = mt.valueContainsNull))
          stack.push(StructField("MapKey", mt.keyType, nullable = false))
        case st: StructType =>
          st.reverseIterator.foreach(stack.push)
        case _ =>
      }
    }
    flattened.toArray
  }

}
