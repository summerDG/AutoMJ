package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, RowOrdering}
import org.apache.spark.sql.catalyst.planning.ExtractEquiTempJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, TempJoin}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

case class TempJoinSelection(conf: SQLConf) extends Strategy with PredicateHelper {
  /**
    * Matches a plan whose output should be small enough to be used in broadcast join.
    */
  private def canBroadcast(plan: LogicalPlan): Boolean = {
    plan.stats(conf).hints.isBroadcastable.getOrElse(false) ||
      (plan.stats(conf).sizeInBytes >= 0 &&
        plan.stats(conf).sizeInBytes <= conf.autoBroadcastJoinThreshold)
  }

  /**
    * Matches a plan whose single partition should be small enough to build a hash table.
    *
    * Note: this assume that the number of partition is fixed, requires additional work if it's
    * dynamic.
    */
  private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
    plan.stats(conf).sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
  }

  /**
    * Returns whether plan a is much smaller (3X) than plan b.
    *
    * The cost to build hash map is higher than sorting, we should only build hash map on a table
    * that is much smaller than other one. Since we does not have the statistic for number of rows,
    * use the size of bytes here as estimation.
    */
  private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
    a.stats(conf).sizeInBytes * 3 <= b.stats(conf).sizeInBytes
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
    case j: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // --- BroadcastHashJoin --------------------------------------------------------------------

    case ExtractEquiTempJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if canBuildRight(joinType) && canBroadcast(right) =>
      Seq(joins.BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

    case ExtractEquiTempJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if canBuildLeft(joinType) && canBroadcast(left) =>
      Seq(joins.BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

    // --- ShuffledHashJoin ---------------------------------------------------------------------

    case ExtractEquiTempJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
        && muchSmaller(right, left) ||
        !RowOrdering.isOrderable(leftKeys) =>
      Seq(joins.ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

    case ExtractEquiTempJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
        && muchSmaller(left, right) ||
        !RowOrdering.isOrderable(leftKeys) =>
      Seq(joins.ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

    // --- SortMergeJoin ------------------------------------------------------------

    case ExtractEquiTempJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
      if RowOrdering.isOrderable(leftKeys) =>
      joins.SortMergeJoinExec(
        leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

    // --- Without joining keys ------------------------------------------------------------

    // Pick BroadcastNestedLoopJoin if one side could be broadcasted
    case j @ TempJoin(left, right, joinType, condition)
      if canBuildRight(joinType) && canBroadcast(right) =>
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), BuildRight, joinType, condition) :: Nil
    case j @ TempJoin(left, right, joinType, condition)
      if canBuildLeft(joinType) && canBroadcast(left) =>
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), BuildLeft, joinType, condition) :: Nil

    // Pick CartesianProduct for InnerJoin
    case TempJoin(left, right, _: InnerLike, condition) =>
      joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

    case TempJoin(left, right, joinType, condition) =>
      val buildSide =
        if (right.stats(conf).sizeInBytes <= left.stats(conf).sizeInBytes) {
          BuildRight
        } else {
          BuildLeft
        }
      // This join could be very slow or OOM
      joins.BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    // --- Cases where this strategy does not apply ---------------------------------------------

    case _ => Nil
  }
}
