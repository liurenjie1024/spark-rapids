package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.DType;

public class ColumnBufferProvider {
  private final SerializedTable table;
  private final SliceInfo sliceInfo;
  private final int columnIdx;
  private final DType dType;
  private final ColumnOffsetInfo offsetInfo;

  public ColumnBufferProvider(SerializedTable table, SliceInfo sliceInfo, int columnIdx,
      DType dType, ColumnOffsetInfo offsetInfo) {
    this.table = table;
    this.sliceInfo = sliceInfo;
    this.columnIdx = columnIdx;
    this.dType = dType;
    this.offsetInfo = offsetInfo;
  }
}
