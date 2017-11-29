package org.apache.spark.sql.catalyst.plans.logical

/**
 * Created by wuxiaoqi on 17-11-28.
 */
abstract class TriNode extends LogicalPlan{
  def left: LogicalPlan
  def middle: LogicalPlan
  def right: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(left, middle, right)
}
