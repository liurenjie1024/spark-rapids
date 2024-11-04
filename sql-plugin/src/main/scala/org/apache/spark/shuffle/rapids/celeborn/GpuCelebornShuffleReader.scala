package org.apache.spark.shuffle.rapids.celeborn

import org.apache.spark.shuffle.ShuffleReader

class GpuCelebornShuffleReader[K, V] extends ShuffleReader[K, V]  {
  override def read(): Iterator[Product2[K, V]] = throw new
    NotImplementedError("celeborn read")
}
