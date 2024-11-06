package org.apache.spark.shuffle.rapids.celeborn

import org.apache.celeborn.common.identity.UserIdentifier

import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.rapids.GpuShuffleDependency

class GpuCelebornShuffleHandle[K, V, C](
    override val appUniqueId: String,
    override val lifecycleManagerHost: String,
    override val lifecycleManagerPort: Int,
    override val userIdentifier: UserIdentifier,
    shuffleId: Int,
    override val throwsFetchFailure: Boolean,
    override val numMappers: Int,
    override val dependency: GpuShuffleDependency[K, V, C],
    override val extension: Array[Byte]) extends CelebornShuffleHandle(
  appUniqueId,
  lifecycleManagerHost,
  lifecycleManagerPort,
  userIdentifier,
  shuffleId,
  throwsFetchFailure,
  numMappers,
  dependency,
  extension) {}
