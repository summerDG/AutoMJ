package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin}
import org.apache.spark.sql.execution.exchange.ShareExchange
import org.apache.spark.sql.execution.joins.LeapFrogJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-12-4.
 */
case class ShareJoinSelection(meta: MetaManager, conf: SQLConf) extends Strategy
  with PredicateHelper{
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ShareJoin(reorderedKeysEachTable, relations, bothKeysEachCondition,
    conditions, numShufflePartitions, shares, dimensionToExprs, closures) =>
      val children: Seq[SparkPlan] = relations.map(n => planLater(n))

      val partitionings: Seq[HcPartitioning] = reorderedKeysEachTable.map {
        case exprs =>
          HcPartitioning(exprs, numShufflePartitions,
            dimensionToExprs.map(_.flatMap(_.keys)), shares)
      }

      val exchanges: Seq[ShareExchange] = for (i <- 0 to relations.length - 1) yield {
        ShareExchange(partitionings(i), reorderedKeysEachTable(i), children(i))
      }

      LeapFrogJoinExec(reorderedKeysEachTable, bothKeysEachCondition,
        conditions, exchanges, numShufflePartitions, closures) :: Nil
  }

  def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions

}
case class KeysAndTableId(keys: Seq[Expression], tableId: Int) {
  override def toString: String = {
    val t = keys.fold("")(_ + "," + _)
    s"tableId: $tableId $t;"
  }
}
