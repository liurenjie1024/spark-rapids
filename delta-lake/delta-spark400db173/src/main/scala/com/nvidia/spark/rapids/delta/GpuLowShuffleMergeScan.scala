/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import java.net.URI
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.broadcast.Broadcast

/** Driver-side information needed to build a low-shuffle target scan. */
case class GpuLowShuffleMergeScanInfo(
    rowIndexMaps: Option[Broadcast[Map[URI, Array[Byte]]]])

/**
 * Bridges information created by the low-shuffle command into Delta file-format conversion.
 * Only a small opaque ID is placed in the logical relation. The broadcast itself is attached to
 * the GPU file format when the scan is converted and is therefore sent to executors normally.
 */
object GpuLowShuffleMergeScanRegistry {
  val OPTION_KEY: String = "spark.rapids.internal.delta.lowShuffleMerge.scanId"

  private val scans = new ConcurrentHashMap[String, GpuLowShuffleMergeScanInfo]()

  def register(info: GpuLowShuffleMergeScanInfo): String = {
    val id = UUID.randomUUID().toString
    scans.put(id, info)
    id
  }

  def lookup(options: Map[String, String]): Option[GpuLowShuffleMergeScanInfo] = {
    options.get(OPTION_KEY).flatMap(id => Option(scans.get(id)))
  }

  def remove(id: String): Unit = scans.remove(id)
}
