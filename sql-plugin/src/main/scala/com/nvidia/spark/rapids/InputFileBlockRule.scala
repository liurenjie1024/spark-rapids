/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Expression, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ShuffleExchangeLike}
import org.apache.spark.sql.rapids.{GpuInputFileBlockLength, GpuInputFileBlockStart, GpuInputFileName}

/**
 * A rule prevents the plans [SparkPlan (with first input_file_xxx expression), FileScan)
 * from running on GPU.
 * For more details, please go to https://github.com/NVIDIA/spark-rapids/issues/3333.
 */
object InputFileBlockRule {
  private type PlanMeta = SparkPlanMeta[SparkPlan]

  def apply(plan: PlanMeta): Unit = {
    // Cache the collected plans in range [SparkPlan(with first input_file_xxx), FileScan).
    // key: the first plan meta containing an input_file_xxx expression.
    // value: the plan metas in range [SparkPlan(with first input_file_xxx), FileScan)
    val fallbackPlans = mutable.LinkedHashMap[PlanMeta, ArrayBuffer[PlanMeta]]()
    collectPlans(plan, fallbackPlans)

    // If we've found some chains, we should prevent the transition.
    fallbackPlans.foreach { case (_, metas) =>
      metas.foreach(_.willNotWorkOnGpu("GPU plans may get incorrect file name" +
        ", or file start or file length from a CPU scan"))
    }
  }

  private def collectPlans(
      planMeta: PlanMeta,
      fallbackPlans: mutable.LinkedHashMap[PlanMeta, ArrayBuffer[PlanMeta]]): Unit = {
    if (hasInputFileExpression(planMeta.wrapped)) {
      // Catch a plan that has an "input_file_xxx", then it becomes a key for the
      // following collecting.
      // 1) Go into children to collect plans for this key.
      var hasAnyCpuFileScan = false
      planMeta.childPlans.foreach { p =>
        hasAnyCpuFileScan = hasAnyCpuFileScan || collectPlansWithKey(p, planMeta, fallbackPlans)
      }
      // 2) Handle the current plan itself.
      // Cache the current plan iff any child has a CPU FileScan and itself will run on GPU.
      if (hasAnyCpuFileScan && planMeta.canThisBeReplaced) {
        fallbackPlans.getOrElseUpdate(planMeta, new ArrayBuffer[PlanMeta]) += planMeta
      }
    } else {
      // Go into children to search a node that has input_file_xxx.
      planMeta.childPlans.foreach(collectPlans(_, fallbackPlans))
    }
  }

  private def collectPlansWithKey(planMeta: PlanMeta, key: PlanMeta,
      fallbackPlans: mutable.LinkedHashMap[PlanMeta, ArrayBuffer[PlanMeta]]): Boolean = {
    var foundCpuFileScan = false
    planMeta.wrapped match {
      case _: ShuffleExchangeLike | _: BroadcastExchangeLike =>
        // Exchange will invalid the input_file_xxx, so the input key is invalid.
        // but need to go into children to look for new plans with input_file_xxx.
        planMeta.childPlans.foreach(collectPlans(_, fallbackPlans))
      case _: FileSourceScanExec | _: BatchScanExec =>
        // Catch a FileScan, check if it can run on GPU, and no need to cache itself.
        foundCpuFileScan = !planMeta.canThisBeReplaced
      case _: LeafExecNode => // Noop, we've reached the LeafNode but find no FileScan
      case _ =>
        // The plan is in the middle of chain [SparkPlan with input_file_xxx, FileScan).
        // NOTE: Here does not check if it has an input_file_xxx expression, so it is
        // ignored because we already have a key in this search context.
        // 1) Check its children to see if there is a child has a CPU FileScan.
        planMeta.childPlans.foreach { p =>
          foundCpuFileScan = foundCpuFileScan || collectPlansWithKey(p, key, fallbackPlans)
        }
        // 2) Cache the plan itself for later fallback iff it will run on GPU and at least
        //    one CPU FileScan exists in its children.
        if (foundCpuFileScan && planMeta.canThisBeReplaced) {
          fallbackPlans.getOrElseUpdate(key, new ArrayBuffer[PlanMeta]) += planMeta
        }
    }
    foundCpuFileScan
  }

  private def hasInputFileExpression(expr: Expression): Boolean = expr match {
    case _: InputFileName => true
    case _: InputFileBlockStart => true
    case _: InputFileBlockLength => true
    case _: GpuInputFileName => true
    case _: GpuInputFileBlockStart => true
    case _: GpuInputFileBlockLength => true
    case e => e.children.exists(hasInputFileExpression)
  }

  /** Whether a plan has any InputFile{Name, BlockStart, BlockLength} expression. */
  def hasInputFileExpression(plan: SparkPlan): Boolean = {
    plan.expressions.exists(hasInputFileExpression)
  }

}