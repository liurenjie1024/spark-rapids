package org.apache.spark.shuffle.rapids.celeborn

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.reflect.DynMethods
import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv, TaskContext}

import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.celeborn.{ExecutorShuffleIdTracker, SendBufferPool, SparkUtils}
import org.apache.spark.shuffle.rapids.celeborn.GpuCelebornManager.executorCores
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.util.Utils

class GpuCelebornManager(private val conf: SparkConf, private val isDriver: Boolean) extends
  Logging {
  private val celebornConf = SparkUtils.fromSparkConf(conf)
  private val shuffleIdTracker = new ExecutorShuffleIdTracker();

  private var appUniqueId: Option[String] = None
  private var shuffleClient: Option[ShuffleClient] = None
  private var lifecycleManager: Option[LifecycleManager] = None
  private val cores: Int = executorCores(conf)
  private val sendBufferPoolCheckInterval = celebornConf.clientPushSendBufferPoolExpireCheckInterval
  private val sendBufferPoolExpireTimeout = celebornConf.clientPushSendBufferPoolExpireTimeout

  def registerShuffle[K, V, C](shuffleId: Int, dependency: GpuShuffleDependency[K, V, C])
  : GpuCelebornShuffleHandle[K, V, C] = {
    appUniqueId = Some(SparkUtils.appUniqueId(dependency.rdd.context))
    initLifecycleManager()

    logWarning(s"Register gpu celeborn shuffle $shuffleId with appUniqueId ${appUniqueId.get}, " +
      s"shuffle id: $shuffleId")

    lifecycleManager.get
      .registerAppShuffleDeterminate(
      shuffleId,
      dependency.rdd.outputDeterministicLevel != DeterministicLevel.INDETERMINATE);

    new GpuCelebornShuffleHandle[K, V, C](
      appUniqueId.get,
      lifecycleManager.get.getHost,
      lifecycleManager.get.getPort,
      lifecycleManager.get.getUserIdentifier,
      shuffleId,
      celebornConf.clientFetchThrowsFetchFailure,
      dependency.rdd.getNumPartitions,
      dependency,
      null)
  }

  def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo(s"Unregister gpu celeborn shuffle  with appUniqueId ${appUniqueId.get}, " +
      s"shuffle id: $shuffleId")
    lifecycleManager.foreach { m =>
      m.unregisterAppShuffle(shuffleId, celebornConf.clientFetchThrowsFetchFailure)
    }

    shuffleClient.foreach(c => shuffleIdTracker.unregisterAppShuffleId(c, shuffleId))

    true
  }

  def stop(): Unit = {
    logInfo(s"Stop gpu celeborn manager with appUniqueId $appUniqueId")
    shuffleClient.foreach { s =>
      s.shutdown()
      ShuffleClient.reset()
    }
    shuffleClient = None

    lifecycleManager.foreach(_.stop())
    lifecycleManager = None
  }

  def getWriter[K, V](
      handle: GpuCelebornShuffleHandle[K, V, V],
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter
  ): GpuCelebornShuffleWriter[K, V] = {
    logInfo(s"Get writer for mapId $mapId, shuffleId ${handle.shuffleId}")
    if (shuffleClient.isEmpty) {
      shuffleClient = Some(ShuffleClient.get(
        handle.appUniqueId,
        handle.lifecycleManagerHost,
        handle.lifecycleManagerPort,
        celebornConf,
        handle.userIdentifier,
        handle.extension))
    }

    new GpuCelebornShuffleWriter[K, V](handle.dependency,
      handle.numMappers,
      context,
      celebornConf,
      shuffleClient.get,
      metrics,
      SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout))
  }

  def getReader[K, C](handle: GpuCelebornShuffleHandle[K, _, C],
      startPartition: Int, endPartition: Int,
      context: TaskContext, metrics: ShuffleReadMetricsReporter): GpuCelebornShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
  }

  def getReader[K, C](handle: GpuCelebornShuffleHandle[K, _, C],
      startMapIndex: Int, endMapIndex: Int, startPartition: Int, endPartition: Int,
      context: TaskContext, metrics: ShuffleReadMetricsReporter): GpuCelebornShuffleReader[K, C] = {
    logInfo(s"Get reader for shuffleId ${handle.shuffleId}, startMapIndex $startMapIndex, " +
      s"endMapIndex $endMapIndex, startPartition $startPartition, endPartition $endPartition")

    new GpuCelebornShuffleReader[K, C](handle, startPartition, endPartition, startMapIndex,
      endMapIndex, context,  celebornConf, metrics, shuffleIdTracker)
  }

  private def initLifecycleManager(): Unit = {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver && lifecycleManager.isEmpty) {
      this.synchronized {
        if (lifecycleManager.isEmpty) {
          lifecycleManager = Some(new LifecycleManager(appUniqueId.get, celebornConf))
          if (celebornConf.clientFetchThrowsFetchFailure) {
            val mapOutputTracker = SparkEnv.get
              .mapOutputTracker
              .asInstanceOf[MapOutputTrackerMaster]
            lifecycleManager.get
              .registerShuffleTrackerCallback((shuffleId: Integer) =>
                SparkUtils.unregisterAllMapOutput(mapOutputTracker, shuffleId))
          }
        }
      }
    }
  }
}

object GpuCelebornManager {
  def executorCores(conf: SparkConf): Int = {
    if (Utils.isLocalMaster(conf)) {
      // SparkContext.numDriverCores is package private.
      DynMethods.builder("numDriverCores")
        .impl("org.apache.spark.SparkContext$", classOf[String])
        .build
        .bind(SparkContext)
        .invoke(conf.get("spark.master"))
    } else {
      conf.getInt(SparkLauncher.EXECUTOR_CORES, 1)
    }
  }
}