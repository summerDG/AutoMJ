package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.JoinType

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class TriJoin(
                    left: LogicalPlan,
                    middle: LogicalPlan,
                    right: LogicalPlan,
                    joinType: JoinType,
                    condition: Option[Expression])
  extends TriNode with PredicateHelper {
  override def output: Seq[Attribute] = ???
}