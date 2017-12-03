package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class JoinSizeEstimator(meta: MetaManager) {
  protected var joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = null
  protected var relations: Seq[LogicalPlan] = null
  def refresh(joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan]): Unit = {
    joinConditions = joins
    relations = tables
  }
  protected def costCore: Long

  def cost(): Long = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    costCore
  }
  def hasArgument: Boolean = {
    joinConditions != null && relations != null
  }
}
