package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class OneRoundStrategy(meta: MetaManager) {
  protected var keysEachRelation: Seq[Seq[Expression]] = null
  protected var joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = null
  protected var relations: Seq[LogicalPlan] = null
  protected var otherCondition: Option[Expression] = None
  protected var partitionNum: Int = 0

  protected var equivalenceClasses: Seq[Seq[Node[AttributeVertex]]] = null

  def refresh(keys: Seq[Seq[Expression]],
              joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan],
              partitions: Int,
              conditions: Option[Expression] = None): Unit = {
    keysEachRelation = keys
    joinConditions = joins
    relations = tables
    otherCondition = conditions
    partitionNum = partitions

    val initEdges = joinConditions.map {
      case ((l, r), (lk, rk)) =>
        (AttributeVertex(l, lk), AttributeVertex(r, rk))
    }.toSeq
    equivalenceClasses = Graph(initEdges).connectComponent()
  }

  protected def optimizeCore: LogicalPlan

  def optimize(): LogicalPlan = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    optimizeCore
  }

  def hasArgument: Boolean = {
    keysEachRelation != null && joinConditions != null && relations != null
  }

  protected def costCore: Long

  def cost(): Long = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    costCore
  }
}
