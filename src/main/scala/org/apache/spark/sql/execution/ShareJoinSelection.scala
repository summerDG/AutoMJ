package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin}
import org.apache.spark.sql.execution.exchange.ShareExchange
import org.apache.spark.sql.execution.joins.LeapFrogJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-12-4.
 */
case class ShareJoinSelection(conf: SQLConf) extends Strategy
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

      val requiredOrderings = partitionings.map(_.expressions.map(SortOrder(_, Ascending)))
      val childrenWithSorter: Seq[SparkPlan] = exchanges.zip(requiredOrderings).map {
        case (child, requiredOrdering) =>
          if (requiredOrdering.nonEmpty) {
            val orderingMatched = if (requiredOrdering.length > child.outputOrdering.length) {
              false
            } else {
              requiredOrdering.zip(child.outputOrdering).forall{
                case (requiredOrder, childOutputOrder) =>
                  childOutputOrder.satisfies(requiredOrder)
              }
            }
            if (!orderingMatched) {
              SortExec(requiredOrdering, global = false, child = child)
            } else {
              child
            }
          } else {
            child
          }
      }

      LeapFrogJoinExec(reorderedKeysEachTable, bothKeysEachCondition,
        conditions, childrenWithSorter, numShufflePartitions, closures) :: Nil
  }

  def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions

}
case class KeysAndTableId(keys: Seq[Expression], tableId: Int) {
  override def toString: String = {
    val t = keys.fold("")(_ + "," + _)
    s"tableId: $tableId $t;"
  }
}
