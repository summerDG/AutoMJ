package org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.KeysAndTableId
import org.apache.spark.sql.execution.joins.ExpressionAndAttributes

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class ShareJoin(keysEachRelation: Seq[Seq[Expression]],
                     relations: Seq[LogicalPlan],
                     bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                     conditions: Option[Expression],
                     numShufflePartitions: Int,
                     shares: Seq[Int],
                     dimensionToExprs: Array[Seq[KeysAndTableId]],
                     closures: Seq[Seq[(ExpressionAndAttributes, Int)]]) extends LogicalPlan{
  override def output: Seq[Attribute] = relations.flatMap(_.output)

  override def children: Seq[LogicalPlan] = relations
}
