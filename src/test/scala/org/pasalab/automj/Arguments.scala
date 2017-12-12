package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-11.
 */
case class Arguments(attributes: Map[String, AttributeReference],
                     keys: Seq[Seq[Expression]],
                     joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                     relations: Seq[LogicalPlan],
                     info: Seq[TableInfo])
