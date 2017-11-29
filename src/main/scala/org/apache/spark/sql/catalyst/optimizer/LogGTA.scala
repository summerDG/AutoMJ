package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.pasalab.automj.MetaManager

/**
 * Created by wuxiaoqi on 17-11-29.
 */
case class LogGTA(meta: MetaManager) extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = ???
}
