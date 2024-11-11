/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.{Aggregator, Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

class GpuShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    val sparkTypes: Array[DataType],
    serializer: Serializer = SparkEnv.get.serializer,
    keyOrdering: Option[Ordering[K]] = None,
    aggregator: Option[Aggregator[K, V, C]] = None,
    mapSideCombine: Boolean = false,
    shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor,
    val useCelebornShuffle: Boolean = false,
    val useGPUShuffle: Boolean,
    val useMultiThreadedShuffle: Boolean,
    val metrics: Map[String, GpuMetric] = Map.empty)
  extends ShuffleDependency[K, V, C](rdd, partitioner, serializer, keyOrdering,
    aggregator, mapSideCombine, shuffleWriterProcessor) {

  private lazy val _sqlMetrics: Map[String, SQLMetric] = {
    GpuMetric.unwrap(metrics)
  }

  def sqlMetrics: Map[String, SQLMetric] = _sqlMetrics

  override def toString: String = "GPU Shuffle Dependency"
}
