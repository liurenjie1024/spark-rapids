package com.nvidia.spark.rapids.shuffle.kudo;

public class WriteMetrics {
  private long calcHeaderTime;
  private long copyHeaderTime;
  private long copyBufferTime;

  public WriteMetrics() {
    this.calcHeaderTime = 0;
    this.copyHeaderTime = 0;
    this.copyBufferTime = 0;
  }

  public long getCalcHeaderTime() {
    return calcHeaderTime;
  }

  public long getCopyBufferTime() {
    return copyBufferTime;
  }

  public void addCopyBufferTime(long time) {
    copyBufferTime += time;
  }

  public long getCopyHeaderTime() {
    return copyHeaderTime;
  }

  public void addCalcHeaderTime(long time) {
    calcHeaderTime += time;
  }

  public void addCopyHeaderTime(long time) {
    copyHeaderTime += time;
  }
}
