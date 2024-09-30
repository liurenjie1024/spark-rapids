/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

package com.nvidia.spark.rapids.velox

import io.glutenproject.rapids.GlutenJniWrapper

object VeloxBackendApis {

  private var runtime: Option[GlutenJniWrapper] = None

  private var isEnabled: Boolean = false

  private var confMap: Map[String, String] = Map()

  private[velox] def init(conf: Map[String, String]): Unit = {
    isEnabled = true
    confMap = conf
  }

  def getRuntime: Option[GlutenJniWrapper] = synchronized {
    runtime match {
      case None if isEnabled =>
        runtime = Some(GlutenJniWrapper.create())
        runtime
      case rt =>
        rt
    }
  }

  def useVeloxHdfs: Boolean = confMap.get("useVeloxHDFS").exists(_.toBoolean)

}
