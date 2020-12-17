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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement.
 */
case class CustomShuffleReaderExec private(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec]) extends UnaryExecNode {
  // If this reader is to read shuffle files locally, then all partition specs should be
  // `PartialMapperPartitionSpec`.
  if (partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])) {
    assert(partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]))
  }

  override def supportsColumnar: Boolean = child.supportsColumnar

  override def output: Seq[Attribute] = child.output
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle reader with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.nonEmpty &&
        partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
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
    } else if (partitionSpecs.length == 1) {
      SinglePartition
    } else if (partitionSpecs.nonEmpty &&
      partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec]) &&
      partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex).toSet.size
        == partitionSpecs.size) {
      // if here, some partitions were coalesced and nothing was split/replicated
      // as in skew mitigation
      child match {
        case a: ShuffleQueryStageExec =>
          a.outputPartitioning match {
            case hp: HashPartitioning => hp.copy(numPartitions = partitionSpecs.length)
            // relies on the fact that coalesce only combines contiguous pre shuffle
            //  partition indexes then
            case rp: RangePartitioning => rp.copy(numPartitions = partitionSpecs.length)
            case _ => UnknownPartitioning(partitionSpecs.length)
          }
        case _ => UnknownPartitioning(partitionSpecs.length)
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  override def stringArgs: Iterator[Any] = {
    val desc = if (isLocalReader) {
      "local"
    } else if (hasCoalescedPartition && hasSkewedPartition) {
      "coalesced and skewed"
    } else if (hasCoalescedPartition) {
      "coalesced"
    } else if (hasSkewedPartition) {
      "skewed"
    } else {
      ""
    }
    Iterator(desc)
  }

  def hasCoalescedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[CoalescedPartitionSpec])

  def hasSkewedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialReducerPartitionSpec])

  def isLocalReader: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  @transient private lazy val partitionDataSizes: Option[Seq[Long]] = {
    if (partitionSpecs.nonEmpty && !isLocalReader && shuffleStage.get.mapStats.isDefined) {
      val bytesByPartitionId = shuffleStage.get.mapStats.get.bytesByPartitionId
      Some(partitionSpecs.map {
        case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
          startReducerIndex.until(endReducerIndex).map(bytesByPartitionId).sum
        case p: PartialReducerPartitionSpec => p.dataSize
        case p => throw new IllegalStateException("unexpected " + p)
      })
    } else {
      None
    }
  }

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val driverAccumUpdates = ArrayBuffer.empty[(Long, Long)]

    val numPartitionsMetric = metrics("numPartitions")
    numPartitionsMetric.set(partitionSpecs.length)
    driverAccumUpdates += (numPartitionsMetric.id -> partitionSpecs.length.toLong)

    if (hasSkewedPartition) {
      val skewedSpecs = partitionSpecs.collect {
        case p: PartialReducerPartitionSpec => p
      }

      val skewedPartitions = metrics("numSkewedPartitions")
      val skewedSplits = metrics("numSkewedSplits")

      val numSkewedPartitions = skewedSpecs.map(_.reducerIndex).distinct.length
      val numSplits = skewedSpecs.length

      skewedPartitions.set(numSkewedPartitions)
      driverAccumUpdates += (skewedPartitions.id -> numSkewedPartitions)

      skewedSplits.set(numSplits)
      driverAccumUpdates += (skewedSplits.id -> numSplits)
    }

    partitionDataSizes.foreach { dataSizes =>
      val partitionDataSizeMetrics = metrics("partitionDataSize")
      driverAccumUpdates ++= dataSizes.map(partitionDataSizeMetrics.id -> _)
      // Set sum value to "partitionDataSize" metric.
      partitionDataSizeMetrics.set(dataSizes.sum)
    }

    SQLMetrics.postDriverMetricsUpdatedByValue(sparkContext, executionId, driverAccumUpdates.toSeq)
  }

  @transient override lazy val metrics: Map[String, SQLMetric] = {
    if (shuffleStage.isDefined) {
      Map("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")) ++ {
        if (isLocalReader) {
          // We split the mapper partition evenly when creating local shuffle reader, so no
          // data size info is available.
          Map.empty
        } else {
          Map("partitionDataSize" ->
            SQLMetrics.createSizeMetric(sparkContext, "partition data size"))
        }
      } ++ {
        if (hasSkewedPartition) {
          Map("numSkewedPartitions" ->
            SQLMetrics.createMetric(sparkContext, "number of skewed partitions"),
            "numSkewedSplits" ->
              SQLMetrics.createMetric(sparkContext, "number of skewed partition splits"))
        } else {
          Map.empty
        }
      }
    } else {
      // It's a canonicalized plan, no need to report metrics.
      Map.empty
    }
  }

  private lazy val shuffleRDD: RDD[_] = {
    sendDriverMetrics()

    shuffleStage.map { stage =>
      stage.shuffle.getShuffleRDD(partitionSpecs.toArray)
    }.getOrElse {
      throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logPartitionSpecs(shuffleStage.flatMap(_.mapStats))
    shuffleRDD.asInstanceOf[RDD[InternalRow]]

  }

  /**
   * Log map side metrics and new partition specs so that we know what got coalesced
   * what got split/replicated due to skew
   */
  private def logPartitionSpecs(mos: Option[MapOutputStatistics]): Unit = {
    val shuffleId = if (mos.isDefined) mos.get.shuffleId else -1
    // include shuffleId to correlate to other logs
    val n = s"(id=${id})"
    logInfo(s"CustomShuffleReaderExec: shuffle(${shuffleId}) $n " +
      metrics.map(kv => (s"${kv._1}=${kv._2.value}")).mkString(","))
    if(shuffleId < 0) {
      return // we don't MapOutputStatistics - probably due to 0-partition RDD
    }
    /* if AQE did nothing, then reduce side tasks would provide info about partition sizes but
    if it rebalanced partitions there is no place what they were before rebalancing
    todo: could we have 100K partitions? */
    logInfo(s"CustomShuffleReaderExec (map): ${mos.get.bytesByPartitionId.mkString(",")}")
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
