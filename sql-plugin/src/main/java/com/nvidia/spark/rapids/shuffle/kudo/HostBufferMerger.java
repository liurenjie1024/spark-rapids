package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import com.nvidia.spark.rapids.shuffle.TableUtils;
import com.nvidia.spark.rapids.shuffle.schema.Visitors;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static com.nvidia.spark.rapids.shuffle.TableUtils.ensure;
import static com.nvidia.spark.rapids.shuffle.TableUtils.getValidityLengthInBytes;
import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.padFor64byteAlignment;
import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.safeLongToInt;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class HostBufferMerger extends MultiTableVisitor<Void, HostMergeResult> {
  // Number of 1s in a byte
  private static final int[] NUMBER_OF_ONES = new int[256];
  private static final byte[] ZEROS = new byte[1024];

  static {
    for (int i = 0; i < NUMBER_OF_ONES.length; i += 1) {
      int count = 0;
      for (int j = 0; j < 8; j += 1) {
        if ((i & (1 << j)) != 0) {
          count += 1;
        }
      }
      NUMBER_OF_ONES[i] = count;
    }

    Arrays.fill(ZEROS, (byte) 0);
  }

  private final List<ColumnOffsetInfo> columnOffsets;
  private final HostMemoryBuffer buffer;
  private final List<ColumnViewInfo> colViewInfoList;

  public HostBufferMerger(List<SerializedTable> tables, HostMemoryBuffer buffer, List<ColumnOffsetInfo> columnOffsets) {
    super(tables);
    requireNonNull(buffer, "buffer can't be null!");
    ensure(columnOffsets != null, "column offsets cannot be null");
    ensure(!columnOffsets.isEmpty(), "column offsets cannot be empty");
    this.columnOffsets = columnOffsets;
    this.buffer = buffer;
    this.colViewInfoList = new ArrayList<>(columnOffsets.size());
  }

  @Override
  protected HostMergeResult doVisitTopSchema(Schema schema, List<Void> children) {
    return new HostMergeResult(buffer, colViewInfoList);
  }

  @Override
  protected Void doVisitStruct(Schema structType, List<Void> children) {
    ColumnOffsetInfo offsetInfo = getCurColumnOffsets();
    long nullCount = deserializeValidityBuffer();
    long totalRowCount = getTotalRowCount();
    colViewInfoList.add(new ColumnViewInfo(structType.getType(),
        offsetInfo, nullCount, totalRowCount));
    return null;
  }

  @Override
  protected Void doPreVisitList(Schema listType) {
    ColumnOffsetInfo offsetInfo = getCurColumnOffsets();
    long nullCount = deserializeValidityBuffer();
    long totalRowCount = getTotalRowCount();
    deserializeOffsetBuffer();

    colViewInfoList.add(new ColumnViewInfo(listType.getType(),
        offsetInfo, nullCount, totalRowCount));
    return null;
  }

  @Override
  protected Void doVisitList(Schema listType, Void preVisitResult, Void childResult) {
    return null;
  }

  @Override
  protected Void doVisit(Schema primitiveType) {
    ColumnOffsetInfo offsetInfo = getCurColumnOffsets();
    long nullCount = deserializeValidityBuffer();
    long totalRowCount = getTotalRowCount();
    if (primitiveType.getType().hasOffsets()) {
      deserializeOffsetBuffer();
      deserializeDataBuffer(OptionalInt.empty());
    } else {
      deserializeDataBuffer(OptionalInt.of(primitiveType.getType().getSizeInBytes()));
    }

    colViewInfoList.add(new ColumnViewInfo(primitiveType.getType(),
        offsetInfo, nullCount, totalRowCount));

    return null;
  }

  private long deserializeValidityBuffer() {
    ColumnOffsetInfo colOffset = getCurColumnOffsets();
    if (colOffset.getValidity().isPresent()) {
      long offset = colOffset.getValidity().getAsLong();
      long validityBufferSize = padFor64byteAlignment(
          getValidityLengthInBytes(getTotalRowCount()));
      try (HostMemoryBuffer validityBuffer = buffer.slice(offset, validityBufferSize)) {
        int nullCountTotal = 0;
        int startRow = 0;
        for (int tableIdx = 0; tableIdx < getTableSize(); tableIdx += 1) {
          SliceInfo sliceInfo = sliceInfoOf(tableIdx);
          long validityOffset = validifyBufferOffset(tableIdx);
          if (validityOffset != -1) {
            nullCountTotal += copyValidityBuffer(validityBuffer, startRow,
                memoryBufferOf(tableIdx), safeLongToInt(validityOffset),
                sliceInfo);
          } else {
            appendAllValid(validityBuffer, startRow, sliceInfo.getRowCount());
          }

          startRow += safeLongToInt(sliceInfo.getRowCount());
        }
        return nullCountTotal;
      }
    } else {
      return 0;
    }
  }

  /**
   * Copy a sliced validity buffer to the destination buffer, starting at the given bit offset.
   *
   * @return Number of nulls in the validity buffer.
   */
  private static int copyValidityBuffer(HostMemoryBuffer dest, int startBit,
      HostMemoryBuffer src, int srcOffset,
      SliceInfo sliceInfo) {
    int nullCount = 0;
    int totalRowCount = safeLongToInt(sliceInfo.getRowCount());
    int curIdx = 0;
    int curSrcByteIdx = srcOffset;
    int curSrcBitIdx = safeLongToInt(sliceInfo.getValidityBufferInfo().getBeginBit());
    int curDestByteIdx = startBit / 8;
    int curDestBitIdx = startBit % 8;

    while (curIdx < totalRowCount) {
      int leftRowCount = totalRowCount - curIdx;
      int appendCount;
      if (curDestBitIdx == 0) {
        appendCount = min(8, leftRowCount);
      } else {
        appendCount = min(8 - curDestBitIdx, leftRowCount);
      }

      int leftBitsInCurSrcByte = 8 - curSrcBitIdx;
      byte srcByte = src.getByte(curSrcByteIdx);
      if (leftBitsInCurSrcByte >= appendCount) {
        // Extract appendCount bits from srcByte, starting from curSrcBitIdx
        byte mask = (byte) (((1 << appendCount) - 1) & 0xFF);
        srcByte = (byte) ((srcByte >>> curSrcBitIdx) & mask);

        nullCount += (appendCount - NUMBER_OF_ONES[srcByte & 0xFF]);

        // Sets the bits in destination buffer starting from curDestBitIdx to 0
        byte destByte = dest.getByte(curDestByteIdx);
        destByte = (byte) (destByte & ((1 << curDestBitIdx) - 1) & 0xFF);

        // Update destination byte with the bits from source byte
        destByte = (byte) ((destByte | (srcByte << curDestBitIdx)) & 0xFF);
        dest.setByte(curDestByteIdx, destByte);

        curSrcBitIdx += appendCount;
        if (curSrcBitIdx == 8) {
          curSrcBitIdx = 0;
          curSrcByteIdx += 1;
        }
      } else {
        // Extract appendCount bits from srcByte, starting from curSrcBitIdx
        byte mask = (byte) (((1 << leftBitsInCurSrcByte) - 1) & 0xFF);
        srcByte = (byte) ((srcByte >>> curSrcBitIdx) & mask);

        byte nextSrcByte = src.getByte(curSrcByteIdx + 1);
        byte nextSrcByteMask = (byte) ((1 << (appendCount - leftBitsInCurSrcByte)) - 1);
        nextSrcByte = (byte) (nextSrcByte & nextSrcByteMask);
        nextSrcByte = (byte) (nextSrcByte << leftBitsInCurSrcByte);
        srcByte = (byte) (srcByte | nextSrcByte);

        nullCount += (appendCount - NUMBER_OF_ONES[srcByte & 0xFF]);

        // Sets the bits in destination buffer starting from curDestBitIdx to 0
        byte destByte = dest.getByte(curDestByteIdx);
        destByte = (byte) (destByte & ((1 << curDestBitIdx) - 1));

        // Update destination byte with the bits from source byte
        destByte = (byte) (destByte | (srcByte << curDestBitIdx));
        dest.setByte(curDestByteIdx, destByte);

        // Update the source byte index and bit index
        curSrcByteIdx += 1;
        curSrcBitIdx = appendCount - leftBitsInCurSrcByte;
      }

      curIdx += appendCount;

      // Update the destination byte index and bit index
      curDestBitIdx += appendCount;
      if (curDestBitIdx == 8) {
        curDestBitIdx = 0;
        curDestByteIdx += 1;
      }
    }

    return nullCount;
  }

  private static void appendAllValid(HostMemoryBuffer dest, int startBit, long numRowsLong) {
    int numRows = safeLongToInt(numRowsLong);
    int curDestByteIdx = startBit / 8;
    int curDestBitIdx = startBit % 8;
    int curIdx = 0;
    while (curIdx < numRows) {
      int leftRowCount = numRows - curIdx;
      int appendCount;
      if (curDestBitIdx == 0) {
        dest.setByte(curDestByteIdx, (byte) 0xFF);
        appendCount = min(8, leftRowCount);
      } else {
        appendCount = min(8 - curDestBitIdx, leftRowCount);
        byte mask = (byte) (((1 << appendCount) - 1) << curDestBitIdx);
        byte destByte = dest.getByte(curDestByteIdx);
        dest.setByte(curDestByteIdx, (byte) (destByte | mask));
      }

      curDestBitIdx += appendCount;
      if (curDestBitIdx == 8) {
        curDestBitIdx = 0;
        curDestByteIdx += 1;
      }

      curIdx += appendCount;
    }
  }

  private void deserializeOffsetBuffer() {
    ColumnOffsetInfo colOffset = getCurColumnOffsets();
    if (colOffset.getOffset().isPresent()) {
      long offset = colOffset.getOffset().getAsLong();
      long bufferSize = Integer.BYTES * (getTotalRowCount() + 1);

      IntBuffer buf = buffer
          .asByteBuffer(offset, safeLongToInt(bufferSize))
          .order(ByteOrder.LITTLE_ENDIAN)
          .asIntBuffer();

      int accumulatedDataLen = 0;

      for (int tableIdx = 0; tableIdx < getTableSize(); tableIdx += 1) {
        SliceInfo sliceInfo = sliceInfoOf(tableIdx);

        if (sliceInfo.getRowCount() > 0) {
          int rowCnt = safeLongToInt(sliceInfo.getRowCount());

          int firstOffset = offsetOf(tableIdx, 0);
          int lastOffset = offsetOf(tableIdx, rowCnt);

          for (int i = 0; i < rowCnt; i += 1) {
            buf.put(offsetOf(tableIdx, i) - firstOffset + accumulatedDataLen);
          }

          accumulatedDataLen += (lastOffset - firstOffset);
        }
      }

      buf.put(accumulatedDataLen);
    }
  }

  private void deserializeDataBuffer(OptionalInt sizeInBytes) {
    ColumnOffsetInfo colOffset = getCurColumnOffsets();

    if (colOffset.getData().isPresent() && colOffset.getDataLen() > 0) {
      long offset = colOffset.getData().getAsLong();
      long dataLen = colOffset.getDataLen();

      try (HostMemoryBuffer buf = buffer.slice(offset, dataLen)) {
        if (sizeInBytes.isPresent()) {
          // Fixed size type
          int elementSize = sizeInBytes.getAsInt();

          long start = 0;
          for (int tableIdx = 0; tableIdx < getTableSize(); tableIdx += 1) {
            SliceInfo sliceInfo = sliceInfoOf(tableIdx);
            if (sliceInfo.getRowCount() > 0) {
              int thisDataLen = safeLongToInt(elementSize * sliceInfo.getRowCount());
              copyDataBuffer(buf, start, tableIdx, thisDataLen);
              start += thisDataLen;
            }
          }
        } else {
          // String type
          long start = 0;
          for (int tableIdx = 0; tableIdx < getTableSize(); tableIdx += 1) {
            int thisDataLen = getStrDataLenOf(tableIdx);
            copyDataBuffer(buf, start, tableIdx, thisDataLen);
            start += thisDataLen;
          }
        }
      }
    }
  }


  private ColumnOffsetInfo getCurColumnOffsets() {
    return columnOffsets.get(getCurrentIdx());
  }

  public static HostMergeResult merge(Schema schema, MergedInfoCalc mergedInfo) {
    List<SerializedTable> serializedTables = mergedInfo.getTables();
    return TableUtils.closeIfException(HostMemoryBuffer.allocate(mergedInfo.getTotalDataLen()),
        buffer -> {
          clearHostBuffer(buffer);
          HostBufferMerger merger = new HostBufferMerger(serializedTables, buffer, mergedInfo.getColumnOffsets());
          return Visitors.visitSchema(schema, merger);
        });
  }

  private static void clearHostBuffer(HostMemoryBuffer buffer) {
    int left = safeLongToInt(buffer.getLength());
    while (left > 0) {
      int toWrite = min(left, ZEROS.length);
      buffer.setBytes(buffer.getLength() - left, ZEROS, 0, toWrite);
      left -= toWrite;
    }
  }
}