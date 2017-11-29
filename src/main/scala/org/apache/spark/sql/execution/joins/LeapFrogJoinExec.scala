package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class LeapFrogJoinExec(keysEachRelation: Seq[Seq[Expression]],
                            joinType: JoinType,
                            bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                            conditions: Option[Expression],
                            relations: Seq[SparkPlan],
                            numShufflePartitions: Int,
                            equivalenceClasses: Seq[Seq[(Int, Int)]]) extends SparkPlan{
  override protected def doExecute(): RDD[InternalRow] = ???

  override def output: Seq[Attribute] = ???

  override def children: Seq[SparkPlan] = ???
}
