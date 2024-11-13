package com.nvidia.spark.rapids.shuffle.kudo;

public class WriteMetrics {
  private long calcHeaderTime;
  private long copyHeaderTime;
  private long copyValidityBufferTime;
  private long copyOffsetBufferTime;
  private long copyDataBufferTime;

  public WriteMetrics() {
    this.calcHeaderTime = 0;
    this.copyHeaderTime = 0;
    this.copyValidityBufferTime = 0;
    this.copyOffsetBufferTime = 0;
    this.copyDataBufferTime = 0;
  }

  public long getCalcHeaderTime() {
    return calcHeaderTime;
  }

  public long getCopyValidityBufferTime() {
    return copyValidityBufferTime;
  }

  public long getCopyOffsetBufferTime() {
    return copyOffsetBufferTime;
  }

  public long getCopyDataBufferTime() {
    return copyDataBufferTime;
  }

  public long getCopyHeaderTime() {
    return copyHeaderTime;
  }

  public void addCalcHeaderTime(long time) {
    calcHeaderTime += time;
  }

  public void addCopyValidityBufferTime(long time) {
    copyValidityBufferTime += time;
  }

  public void addCopyOffsetBufferTime(long time) {
    copyOffsetBufferTime += time;
  }

  public void addCopyDataBufferTime(long time) {
    copyDataBufferTime += time;
  }

  public void addCopyHeaderTime(long time) {
    copyHeaderTime += time;
  }
}
