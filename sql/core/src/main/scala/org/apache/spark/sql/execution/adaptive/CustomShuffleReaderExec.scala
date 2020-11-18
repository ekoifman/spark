/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement.
 * @param description     The string description of this shuffle reader.
 */
case class CustomShuffleReaderExec private(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec],
    description: String) extends UnaryExecNode {

  override def supportsColumnar: Boolean = child.supportsColumnar

  override def output: Seq[Attribute] = child.output
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle reader with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
          partitionSpecs.length) {
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeLike) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeLike)) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }
  /**
   * include id so that it is visible in the plan text
   */
  override def stringArgs: Iterator[Any] = Iterator(id, description)

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage.map { stage =>
      stage.shuffle.getShuffleRDD(partitionSpecs.toArray)
    }.getOrElse {
      throw new IllegalStateException("operating on canonicalized plan")
    }
  }
  override lazy val metrics = Map(
    "map side partitions" -> SQLMetrics.createMetric(sparkContext, "map side partitions"),
    "map side min" -> SQLMetrics.createMetric(sparkContext, "map side min"),
    "map side avg" -> SQLMetrics.createMetric(sparkContext, "map side avg"),
    "map side max" -> SQLMetrics.createMetric(sparkContext, "map side max"),
    "reduce side partitions" -> SQLMetrics.createMetric(sparkContext, "reduce side partitions"),
    /* This is just to map this SQL operator to a stage. In the UI it shows up, don't know
    * about SWH.  If all Exchange node metrics showed up in SWH this is not needed as
    * they would also tell us map and reduce side stage IDs */
    "reduce stage" -> SQLMetrics.createMetric(sparkContext, "reduce stage"))

  override protected def doExecute(): RDD[InternalRow] = {
    val v = shuffleStage.flatMap(_.mapStats).get // should always be there
    longMetric("map side partitions") += v.bytesByPartitionId.length
    longMetric("map side min") += v.bytesByPartitionId.min
    longMetric("map side avg") += v.bytesByPartitionId.sum/v.bytesByPartitionId.length
    longMetric("map side max") += v.bytesByPartitionId.max
    longMetric("reduce side partitions") += partitionSpecs.length
    logPartitionSpecs(v)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
    val stageId = longMetric("reduce stage")
    shuffleRDD.asInstanceOf[RDD[InternalRow]].mapPartitions( iter => {
      stageId += 0
      iter}, true)
  }

  /**
   * Log map side metrics and new partition specs so that we know what got coalesced
   * what got split/replicated due to skew
   */
  private def logPartitionSpecs(mos: MapOutputStatistics): Unit = {
    // include shuffleId to correlate to other logs
    val n = s"(id=${id})"
    logInfo(s"CustomShuffleReaderExec: shuffle(${mos.shuffleId}) $n " +
      metrics.map(kv => (s"${kv._1}=${kv._2.value}")).mkString(","))
    /* if AQE did nothing, then reduce side tasks would provide info about partition sizes but
    if it rebalanced partitions there is no place what they were before rebalancing
    todo: could we have 100K partitions? */
    logInfo(s"CustomShuffleReaderExec (map): ${mos.bytesByPartitionId.mkString(",")}")
    val specs = partitionSpecs.map {
      case a : CoalescedPartitionSpec => s"CP(${a.startReducerIndex},${a.endReducerIndex})"
      case a : PartialReducerPartitionSpec =>
        s"PRP(${a.reducerIndex},${a.startMapIndex},${a.endMapIndex})"
      case a : PartialMapperPartitionSpec =>
        s"PMP(${a.mapIndex},${a.startReducerIndex},${a.endReducerIndex})"
      case a => s"$a"
    }.mkString("[", ",", "]")
    logInfo(s"CustomShuffleReaderExec (reduce): $specs")
  }
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    shuffleRDD.asInstanceOf[RDD[ColumnarBatch]]
  }
}
