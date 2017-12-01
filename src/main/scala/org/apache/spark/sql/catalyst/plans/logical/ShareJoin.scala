package org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.pasalab.automj.{AttributeVertex, Node}

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class ShareJoin(keysEachRelation: Seq[Seq[Expression]],
                     bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                     conditions: Option[Expression],
                     relations: Seq[LogicalPlan],
                     numShufflePartitions: Int,
                     equivalenceClasses: Seq[Seq[Node[AttributeVertex]]]) extends LogicalPlan{
  override def output: Seq[Attribute] = ???

  override def children: Seq[LogicalPlan] = ???
}
