package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.{ShareExchange, ShuffleExchange}
import org.apache.spark.sql.execution.joins.LeapFrogJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by wuxiaoqi on 17-12-4.
 */
case class ShareJoinSelection(conf: SQLConf) extends Strategy with PredicateHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ShareJoin(keysEachRelation, bothKeysEachCondition, conditions,
    relations, numShufflePartitions, equivalenceClasses) =>
      val requiredDistributionsWithId = keysEachRelation
        .foldRight(List[Distribution]())((keys, s) => ClusteredDistribution(keys) :: s).zipWithIndex
      val children: Seq[SparkPlan] = relations.map(n => planLater(n))
      val exchanges = children.zip(keysEachRelation).zip(requiredDistributionsWithId ).map {
        case ((child, projExprs), (dist, id)) =>
          val puppetPartitioning = createPartitioning(dist, defaultNumPreShufflePartitions)
          ShareExchange(puppetPartitioning, projExprs, child, id)
      }
      LeapFrogJoinExec(keysEachRelation, bothKeysEachCondition,
        conditions, exchanges, numShufflePartitions, equivalenceClasses) :: Nil
  }
  def createPartitioning(requiredDistribution: Distribution, numPartitions: Int): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering) => HashPartitioning(clustering, numPartitions)
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }
  def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions
}
