package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans._

case class TempJoin(
                     left: LogicalPlan,
                     right: LogicalPlan,
                     joinType: JoinType,
                     condition: Option[Expression]) extends BinaryNode with PredicateHelper{
  override def output: Seq[Attribute] = {
    joinType match {
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case _ =>
        left.output ++ right.output
    }
  }
}
