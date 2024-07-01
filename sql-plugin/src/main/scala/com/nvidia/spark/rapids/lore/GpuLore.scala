/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuCustomShuffleReaderExec}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration

case class LoreRDDMeta(numPartitions: Int, outputPartitions: Seq[Int], attrs: Seq[Attribute])

case class LoreRDDPartitionMeta(numBatches: Int, dataType: Seq[DataType])

trait GpuLoreRDD {
  def rootPath: Path

  def pathOfMeta: Path = new Path(rootPath, "rdd.meta")

  def pathOfPartition(partitionIndex: Int): Path = {
    new Path(rootPath, s"partition-$partitionIndex")
  }

  def pathOfPartitionMeta(partitionIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), "partition.meta")
  }

  def pathOfBatch(partitionIndex: Int, batchIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), s"batch-$batchIndex.parquet")
  }
}


object GpuLore {
  /**
   * Lore id of a plan node.
   */
  val LORE_ID_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.id")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a root node whose meta and
   * input should be dumped.
   */
  val LORE_DUMP_PATH_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.dump.path")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a child node whose data
   * should be dumped.
   */
  val LORE_DUMP_RDD_TAG: TreeNodeTag[LoreDumpRDDInfo] = new TreeNodeTag[LoreDumpRDDInfo](
    "rapids.gpu.lore.dump.rdd.info")

  def pathOfRootPlanMeta(rootPath: Path): Path = {
    new Path(rootPath, "plan.meta")
  }

  def dumpPlan[T <: SparkPlan : ClassTag](plan: T, rootPath: Path): Unit = {
    dumpObject(plan, pathOfRootPlanMeta(rootPath),
      SparkShimImpl.sessionFromPlan(plan).sparkContext.hadoopConfiguration)
  }

  def dumpObject[T: ClassTag](obj: T, path: Path, hadoopConf: Configuration): Unit = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.create(path, false)) { fout =>
        val serializerStream = SparkEnv.get.serializer.newInstance().serializeStream(fout)
        withResource(serializerStream) { ser =>
          ser.writeObject(obj)
        }
      }
    }
  }

  def loadObject[T: ClassTag](path: Path, hadoopConf: Configuration): T = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.open(path)) { fin =>
        val serializerStream = SparkEnv.get.serializer.newInstance().deserializeStream(fin)
        withResource(serializerStream) { ser =>
          ser.readObject().asInstanceOf[T]
        }
      }
    }
  }

  def pathOfChild(rootPath: Path, childIndex: Int): Path = {
    new Path(rootPath, s"input-$childIndex")
  }

  def restoreGpuExec(rootPath: Path, spark: SparkSession): GpuExec = {
    val rootExec = loadObject[GpuExec](pathOfRootPlanMeta(rootPath),
      spark.sparkContext.hadoopConfiguration)

    checkUnsupportedOperator(rootExec)

    val broadcastHadoopConf = {
      val sc = spark.sparkContext
      sc.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
    }

    // Load children
    val newChildren = rootExec.children.zipWithIndex.map { case (plan, idx) =>
      val newChild = GpuLoreReplayExec(idx, rootPath.toString, broadcastHadoopConf)
      plan match {
        case b: GpuBroadcastExchangeExec =>
          b.withNewChildren(Seq(newChild))
        case b: BroadcastQueryStageExec =>
          b.broadcast.withNewChildren(Seq(newChild))
        case _ => newChild
      }
    }

    var nextId = rootExec.children.length

    rootExec.transformExpressionsUpWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case sub: ExecSubqueryExpression =>
        nextId += 1
        restoreSubqueryPlan(nextId, sub, rootPath, broadcastHadoopConf)
    }.withNewChildren(newChildren).asInstanceOf[GpuExec]

//    rootExec match {
//      case b: GpuFilterExec =>
//        val newExpr = restoreSubqueryPlan(1, b.condition, rootPath, broadcastHadoopConf)._1
//        b.makeCopy(Array(newExpr, newChildren.head)).asInstanceOf[GpuExec]
//      case p: GpuProjectExec =>
//        var nextPlanId = 1
//        val newExprs = p.expressions.map { expr =>
//          val (newExpr, nextId) = restoreSubqueryPlan(nextPlanId, expr, rootPath
//            , broadcastHadoopConf)
//          nextPlanId = nextId
//          newExpr
//        }.toList
//
//        p.makeCopy(Array(newExprs, newChildren.head)).asInstanceOf[GpuExec]
//      case _ => rootExec.withNewChildren(newChildren)
//        .asInstanceOf[GpuExec]
//    }
  }

  private def restoreSubqueryPlan(id: Int, sub: ExecSubqueryExpression,
      rootPath: Path, hadoopConf: Broadcast[SerializableConfiguration]): ExecSubqueryExpression = {
    val innerPlan = sub.plan.child

    if (innerPlan.isInstanceOf[GpuExec]) {
      var newChild: SparkPlan = GpuLoreReplayExec(id, rootPath.toString, hadoopConf)

      if (!innerPlan.supportsColumnar) {
        newChild = GpuColumnarToRowExec(newChild)
      }
      val newSubqueryExec = sub.plan match {
        case ReusedSubqueryExec(subqueryExec) => subqueryExec.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
        case p: BaseSubqueryExec => p.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
      }
      sub.withNewPlan(newSubqueryExec)
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  /**
   * Lore id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOf(plan: SparkPlan): Option[Int] = {
    // When the execution id is not set, it means there is no actual execution happening, in this
    // case we don't need to generate lore id.
    Option(SparkShimImpl.sessionFromPlan(plan)
      .sparkContext
      .getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map { executionId =>
        idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
      }
  }

  def tagForLore(sparkPlan: SparkPlan, rapidsConf: RapidsConf): SparkPlan = {
    val loreDumpIds = rapidsConf.loreDumpIds

    val newPlan = if (loreDumpIds.nonEmpty) {
      // We need to dump the output of nodes with the lore id in the dump ids
      val loreOutputRootPath = rapidsConf.loreDumpPath.getOrElse(throw
        new IllegalArgumentException(s"${RapidsConf.LORE_DUMP_PATH.key} must be set " +
          s"when ${RapidsConf.LORE_DUMP_IDS.key} is set."))

      val hadoopConf = {
        val sc = SparkShimImpl.sessionFromPlan(sparkPlan).sparkContext
        sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
      }

      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)

            loreDumpIds.get(loreId).foreach { outputLoreIds =>
              checkUnsupportedOperator(g)
              val currentExecRootPath = new Path(loreOutputRootPath, s"loreId-$loreId")
              g.setTagValue(LORE_DUMP_PATH_TAG, currentExecRootPath.toString)
              val loreOutputInfo = LoreOutputInfo(outputLoreIds,
                currentExecRootPath.toString)

              g.children.zipWithIndex.foreach {
                case (child, idx) =>
                  val dumpRDDInfo = LoreDumpRDDInfo(idx, loreOutputInfo, child.output, hadoopConf)
                  child match {
                    case c: BroadcastQueryStageExec =>
                      c.broadcast.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
                    case o => o.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
                  }
              }

              var nextId = g.children.length
              g.transformExpressionsUpWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
                case sub: ExecSubqueryExpression =>
                  tagSubqueryPlan(nextId, sub, loreOutputInfo, hadoopConf)
                  nextId += 1
                  sub
              }

//              g match {
//                case f: GpuFilterExec =>
//                  tagForSubqueryPlan(1, f.condition, loreOutputInfo, hadoopConf)
//                case p: GpuProjectExec =>
//                  p.projectList.foldLeft(1)((nextPlanId, expr) =>
//                    tagForSubqueryPlan(nextPlanId, expr, loreOutputInfo, hadoopConf))
//                case agg: GpuHashAggregateExec =>
//                  agg.aggregateExpressions.flatMap(_.aggregateFunction.children).collect {
//                    // The reason we can't support dumping subquery expression in aggregate
//                    // function is that typically aggregation function will be split into
//                    // partial aggregation and final aggregation, and the
//                    // [ReuseExchangeAndSubquery] rule will replace final aggregation's subquery
//                    // with reused subquery expression. The problem is this rule happens in
//                    // last step, even after columnar rule, so the tag will no longer work. We
//                    // may add some physical rules to handle this in future, but given that
//                    // this is a corner case, we don't support it for now.
//                    case _: ExecSubqueryExpression => throw new IllegalArgumentException(
//                      "Unable to support dumping subquery expression in aggregate function")
//                  }
//                case _ =>
//              }
            }
          }
        case _ =>
      }

      println("===Being of lore===")
      println(s"${sparkPlan.treeString}")
      println("===End of lore===")

      sparkPlan

    } else {
      // We don't need to dump the output of the nodes, just tag the lore id
      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)
          }
        case _ =>
      }

      sparkPlan
    }

    newPlan
  }

  def loreIdOf(node: SparkPlan): Option[String] = {
    node.getTagValue(LORE_ID_TAG)
  }

  private def tagSubqueryPlan(id: Int, sub: ExecSubqueryExpression,
      loreOutputInfo: LoreOutputInfo, hadoopConf: Broadcast[SerializableConfiguration]) = {
    val innerPlan = sub.plan.child
    if (innerPlan.isInstanceOf[GpuExec]) {
      val dumpRDDInfo = LoreDumpRDDInfo(id, loreOutputInfo, innerPlan.output,
        hadoopConf)
      innerPlan match {
        case p: GpuColumnarToRowExec => p.child.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
        case c => c.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
      }
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  private def checkUnsupportedOperator(plan: SparkPlan): Unit = {
    if (plan.children.isEmpty ||
      plan.isInstanceOf[GpuCustomShuffleReaderExec]
    ) {
      throw new UnsupportedOperationException(s"Currently we don't support dumping input of " +
        s"${plan.getClass.getSimpleName} operator.")
    }
  }
}
