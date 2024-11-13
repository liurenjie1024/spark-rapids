package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.BufferType;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.shuffle.TableUtils;
import com.nvidia.spark.rapids.shuffle.schema.HostColumnsVisitor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.padForHostAlignment;


class SlicedBufferSerializer implements HostColumnsVisitor<Void> {
  private final SliceInfo root;
  private final BufferType bufferType;
  private final DataWriter writer;
  private final WriteMetrics metrics;

  private final Deque<SliceInfo> sliceInfos = new ArrayDeque<>();
  private long totalDataLen;

  SlicedBufferSerializer(long rowOffset, long numRows, BufferType bufferType, DataWriter writer
      , WriteMetrics metrics) {
    this.root = new SliceInfo(rowOffset, numRows);
    this.bufferType = bufferType;
    this.writer = writer;
    this.metrics = metrics;
    this.sliceInfos.addLast(root);
    this.totalDataLen = 0;
  }

  public long getTotalDataLen() {
    return totalDataLen;
  }

  @Override
  public Void visitStruct(HostColumnVectorCore col, List<Void> children) {
    SliceInfo parent = sliceInfos.peekLast();

    try {
      switch (bufferType) {
      case VALIDITY:
        totalDataLen += this.copySlicedValidity(col, parent);
        return null;
      case OFFSET:
      case DATA:
        return null;
      default:
        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Void preVisitList(HostColumnVectorCore col) {
    SliceInfo parent = sliceInfos.getLast();


    long bytesCopied = 0;
    try {
      switch (bufferType) {
      case VALIDITY:
        bytesCopied = this.copySlicedValidity(col, parent);
        break;
      case OFFSET:
        bytesCopied = this.copySlicedOffset(col, parent);
        break;
      case DATA:
        break;
      default:
        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    SliceInfo current;
    if (col.getOffsets() != null) {
      long start = col.getOffsets()
          .getInt(parent.offset * Integer.BYTES);
      long end = col.getOffsets().getInt((parent.offset + parent.rowCount) * Integer.BYTES);
      long rowCount = end - start;

      current = new SliceInfo(start, rowCount);
    } else {
      current = new SliceInfo(0, 0);
    }

    sliceInfos.addLast(current);

    totalDataLen += bytesCopied;
    return null;
  }

  @Override
  public Void visitList(HostColumnVectorCore col, Void preVisitResult, Void childResult) {
    sliceInfos.removeLast();
    return null;
  }

  @Override
  public Void visit(HostColumnVectorCore col) {
    SliceInfo parent = sliceInfos.getLast();
    try {
      switch (bufferType) {
      case VALIDITY:
        totalDataLen += this.copySlicedValidity(col, parent);
        return null;
      case OFFSET:
        totalDataLen += this.copySlicedOffset(col, parent);
        return null;
      case DATA:
        totalDataLen += this.copySlicedData(col, parent);
        return null;
      default:
        throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private long copySlicedValidity(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
    if (column.getValidity() != null && sliceInfo.getRowCount() > 0) {
      HostMemoryBuffer buff = column.getValidity();
      long len = sliceInfo.getValidityBufferInfo().getBufferLength();
      return TableUtils.withTime(() -> {
        try {
          writer.copyDataFrom(buff, sliceInfo.getValidityBufferInfo().getBufferOffset(),
              len);
          return padForHostAlignment(writer, len);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, metrics::addCopyValidityBufferTime);
    } else {
      return 0;
    }
  }

  private long copySlicedOffset(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
    if (sliceInfo.rowCount <= 0 || column.getOffsets() == null) {
      // Don't copy anything, there are no rows
      return 0;
    }
    long bytesToCopy = (sliceInfo.rowCount + 1) * Integer.BYTES;
    long srcOffset = sliceInfo.offset * Integer.BYTES;
    HostMemoryBuffer buff = column.getOffsets();
    return TableUtils.withTime(() -> {
      try {
        writer.copyDataFrom(buff, srcOffset, bytesToCopy);
        return padForHostAlignment(writer, bytesToCopy);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, metrics::addCopyOffsetBufferTime);
  }

  private long copySlicedData(HostColumnVectorCore column, SliceInfo sliceInfo) throws IOException {
    if (sliceInfo.rowCount > 0) {
      DType type = column.getType();
      if (type.equals(DType.STRING)) {
        long startByteOffset = column.getOffsets().getInt(sliceInfo.offset * Integer.BYTES);
        long endByteOffset = column.getOffsets().getInt((sliceInfo.offset + sliceInfo.rowCount) * Integer.BYTES);
        long bytesToCopy = endByteOffset - startByteOffset;
        if (column.getData() == null) {
          if (bytesToCopy != 0) {
            throw new IllegalStateException("String column has no data buffer, " +
                "but bytes to copy is not zero: " + bytesToCopy);
          }

          return 0;
        } else {
          return TableUtils.withTime(() -> {
            try {
              writer.copyDataFrom(column.getData(), startByteOffset, bytesToCopy);
              return padForHostAlignment(writer, bytesToCopy);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }, metrics::addCopyDataBufferTime);
        }
      } else if (type.getSizeInBytes() > 0) {
        long bytesToCopy = sliceInfo.rowCount * type.getSizeInBytes();
        long srcOffset = sliceInfo.offset * type.getSizeInBytes();

        return TableUtils.withTime(() -> {
          try {
            writer.copyDataFrom(column.getData(), srcOffset, bytesToCopy);
            return padForHostAlignment(writer, bytesToCopy);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, metrics::addCopyDataBufferTime);
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }
}
