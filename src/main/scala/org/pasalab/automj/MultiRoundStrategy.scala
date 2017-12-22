package org.pasalab.automj

import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by wuxiaoqi on 17-12-1.
 */
abstract class MultiRoundStrategy(conf: SQLConf) {
  def optimize(joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
               relations: Seq[LogicalPlan]): LogicalPlan
}
