package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-11-30.
 */
case class HyperGraphEdge(relation: LogicalPlan,
                          vertices: Seq[Int]) extends PredicateHelper{
  def joinKeys: Seq[Seq[Expression]] = {
    vertices.map(_.equivalenceClass.filter(e => canEvaluate(e, relation)))
  }
  def vertexIds: Seq[Int] = vertices
}
