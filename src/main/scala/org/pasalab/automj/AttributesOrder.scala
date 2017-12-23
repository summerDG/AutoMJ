package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.KeysAndTableId

/**
 * Created by wuxiaoqi on 17-12-5.
 */
trait AttributesOrder {
  def attrOptimization(closureLength: Int,
                       relations: Seq[LogicalPlan],
                       statistics: Seq[Statistics],
                       exprToCid: Map[ExprId, Int]): Array[Seq[KeysAndTableId]]
}
