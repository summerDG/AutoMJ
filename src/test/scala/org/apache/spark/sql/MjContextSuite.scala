package org.apache.spark.sql

import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.execution.{ShareJoinSelection, TempJoinSelection}
import org.apache.spark.sql.test.SharedSQLContext
import org.pasalab.automj.MjConfigConst

/**
 * Created by wuxiaoqi on 17-12-10.
 */
class MjContextSuite extends SharedSQLContext{
  test("strategy test") {
    val conf = sparkContext.getConf
      .set(MjConfigConst.ONE_ROUND_STRATEGY, "org.pasalab.automj.ShareStrategy")
      .set(MjConfigConst.MULTI_ROUND_STRATEGY, "org.pasalab.automj.LeftDepthStrategy")
      .set(MjConfigConst.JOIN_SIZE_ESTIMATOR, "org.pasalab.automj.EstimatorBasedSample")
      .set(MjConfigConst.ONE_ROUND_ONCE, "true")
    val mjSession = new MjSession(sparkContext)
    val optimizer = mjSession.sessionState.optimizer.extendedOperatorOptimizationRules.head

    assert(optimizer.isInstanceOf[MjOptimizer], "Not MjOptimizer!!!")
    optimizer match {
      case m: MjOptimizer =>
        assert(m.multiRoundStrategy.isDefined, "not define multiRoundStrategy")
        assert(m.oneRoundStrategy.isDefined, "not define oneRoundStrategy")
        assert(m.joinSizeEstimator.isDefined, "not define joinSizeEstimator")
    }

    val strategy1 = mjSession.sessionState.planner.extraPlanningStrategies(0)
    assert(strategy1.isInstanceOf[TempJoinSelection], "Not TempJoinSelection!!!")
    val strategy2 = mjSession.sessionState.planner.extraPlanningStrategies(1)
    assert(strategy2.isInstanceOf[ShareJoinSelection], "Not ShareJoinSelection!!!")
  }
}
