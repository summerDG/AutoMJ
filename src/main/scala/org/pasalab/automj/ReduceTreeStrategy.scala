package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-1.
 */
abstract class ReduceTreeStrategy(meta: MetaManager) {
  def reduce(joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])], relations: Seq[LogicalPlan]): LogicalPlan
}
