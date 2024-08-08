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

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer}
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

case class VectorBuilder(posInSchema: Int,
                         isRoot: Boolean,
                         field: StructField,
                         nullBuffer: Option[HostMemoryBuffer],
                         dataBuffer: Option[HostMemoryBuffer],
                         offsetBuffer: Option[HostMemoryBuffer],
                         children: Seq[VectorBuilder]) {

  def build(tailInfo: Array[Long]): HostColumnVectorCore = {
    var childVecs = new java.util.ArrayList[HostColumnVectorCore]()
    children.foreach(b => childVecs.add(b.build(tailInfo)))

    val dType = VeloxBatchConverter.mapSparkTypeToDType(field.dataType)
    val rowCount = tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 2))
    val nullCount = java.util.Optional.of(java.lang.Long.valueOf(
      tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 3))))

    val finalDataBuffer = dataBuffer.map { buf =>
      val f = buf.slice(0, tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 0)))
      buf.close()
      f
    }
    val finalOffsetBuffer = offsetBuffer.map { buf =>
      val f = buf.slice(0, tailInfo(VectorBuilder.getTailInfoPos(posInSchema, 1)))
      buf.close()
      f
    }
    val finalNullBuffer = nullBuffer.map { buf =>
      val f = buf.slice(0, VeloxBatchConverter.sizeOfNullMask(rowCount.toInt))
      buf.close()
      f
    }

    // Cast Map[child0, child1] => List[Struct[child0, child1]]
    if (field.dataType.isInstanceOf[MapType]) {
      val structCol = new HostColumnVectorCore(DType.STRUCT,
        childVecs.get(0).getRowCount, java.util.Optional.of(0L),
        null, null, null, childVecs)
      childVecs = new java.util.ArrayList[HostColumnVectorCore]()
      childVecs.add(structCol)
    }

    if (isRoot) {
      new HostColumnVector(dType, rowCount, nullCount,
        finalDataBuffer.orNull, finalNullBuffer.orNull, finalOffsetBuffer.orNull,
        childVecs)
    } else {
      new HostColumnVectorCore(dType, rowCount, nullCount,
        finalDataBuffer.orNull, finalNullBuffer.orNull, finalOffsetBuffer.orNull,
        childVecs)
    }
  }
}

object VectorBuilder {
  private val TAIL_INFO_OFFSET = 2
  private val TAIL_INFO_STRIDE = 4

  private def getTailInfoPos(colIndex: Int, infoIndex: Int): Int = {
    colIndex * TAIL_INFO_STRIDE + TAIL_INFO_OFFSET + infoIndex
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

  def close(): Unit = {
    require(!deckFilled, "The deck is NOT empty")
    require(columnBuilders.isEmpty, "Please flush existing ColumnBuilders at first")
    runtime.closeCoalesceConverter(nativeHandle)
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
      val vecBuilder = createVectorBuilder(
        bufferPtrs,
        estimatedBatchNum,
        rootInfo,
        isRoot = true)
      columnBuilders += vecBuilder
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
  def flushAndConvert(): Array[HostColumnVector] = {
    require(columnBuilders.nonEmpty, "ColumnBuilders has NOT been setup")
    val start: Long = System.nanoTime()

    val tailInfo = runtime.flush(nativeHandle)
    metrics("OutputSizeInBytes") += tailInfo(1)
    val ret = columnBuilders.map(_.build(tailInfo).asInstanceOf[HostColumnVector])
    columnBuilders.clear()

    eclipsed += System.nanoTime() - start
    ret.toArray
  }

  private def createVectorBuilder(bufferPtrs: mutable.ArrayBuffer[Long],
                                  estimatedBatchNum: Double,
                                  info: SampleColumnInfo,
                                  isRoot: Boolean): VectorBuilder = {

    require(VeloxDataTypes.canConvert(info.veloxType, info.readType.dataType),
      s"can NOT convert ${info.veloxType} to ${info.readType.dataType}")

    val nullBuffer = if (info.readType.nullable) {
      val estimatedRows = (info.numRows * estimatedBatchNum).toInt
      val nullMaskBytes = VeloxBatchConverter.sizeOfNullMask(estimatedRows)
      Some(HostMemoryBuffer.allocate(nullMaskBytes))
    } else {
      None
    }
    val dataBuffer = if (info.dataSize > 0) {
      val estimatedDataSize = (info.dataSize * estimatedBatchNum).toInt
      Some(HostMemoryBuffer.allocate(estimatedDataSize))
    } else {
      None
    }
    val offsetBuffer = if (info.offsetsSize > 0) {
      val estimatedOffsetSize = (info.offsetsSize * estimatedBatchNum).toInt
      Some(HostMemoryBuffer.allocate(estimatedOffsetSize))
    } else {
      None
    }

    // bufferPtrs is in pre-order
    val typeIdx = VeloxDataTypes.encodeSparkType(info.readType.dataType).toLong
    bufferPtrs.append(
      typeIdx,
      dataBuffer.map(_.getAddress).getOrElse(0L),
      dataBuffer.map(_.getLength).getOrElse(0L),
      nullBuffer.map(_.getAddress).getOrElse(0L),
      nullBuffer.map(_.getLength).getOrElse(0L),
      offsetBuffer.map(_.getAddress).getOrElse(0L),
      offsetBuffer.map(_.getLength).getOrElse(0L),
    )

    val childBuilders = info.children.map(ch => createVectorBuilder(
      bufferPtrs, estimatedBatchNum, ch, isRoot = false))

    VectorBuilder(info.posInSchema, isRoot, info.readType,
      nullBuffer, dataBuffer, offsetBuffer,
      childBuilders)
  }
}

object VeloxBatchConverter extends Logging {

  def apply(firstBatch: ColumnarBatch,
            targetBatchSize: Int,
            schema: StructType,
            metrics: Map[String, SQLMetric]): VeloxBatchConverter = {
    val runtime = GlutenJniWrapper.create()
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
    val sb = mutable.StringBuilder.newBuilder
    sb.append("==HEAD== ").append((0 until offset).map(array).mkString(" | ")).append('\n')
    (offset until array.length by step).foreach { i =>
      sb.append(s"  (${(i - offset) / step + 1}) ")
      sb.append((i until i + step).map(array).mkString(" | "))
      sb.append('\n')
    }
    logInfo(s"$title: \n${sb.toString()}")
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
