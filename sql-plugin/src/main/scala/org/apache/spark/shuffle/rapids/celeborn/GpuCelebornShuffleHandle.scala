package org.apache.spark.shuffle.rapids.celeborn

import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.sql.rapids.GpuShuffleDependency

class GpuCelebornShuffleHandle[K, V, C](
    override val dependency: GpuShuffleDependency[K, V, C]) extends
  BaseShuffleHandle[K, V, C](dependency.shuffleId, dependency) {
  override def toString: String = s"Celeborn Shuffle Handle $shuffleId"
}
