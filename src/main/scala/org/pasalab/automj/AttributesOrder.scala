package org.pasalab.automj

import org.apache.spark.{MjStatistics, SampleStat}
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.KeysAndTableId

/**
 * Created by wuxiaoqi on 17-12-5.
 */
trait AttributesOrder {
  def attrOptimization(closureLength: Int,
                       relations: Seq[LogicalPlan],
                       statistics: Seq[MjStatistics],
                       exprToCid: Map[Long, Int]): Array[Seq[KeysAndTableId]]
  def attrOptimization(samples: Seq[SampleStat[Any]],
                       equivalenceClasses: Seq[Seq[Node[AttributeVertex]]]): Array[Seq[KeysAndTableId]]
}
