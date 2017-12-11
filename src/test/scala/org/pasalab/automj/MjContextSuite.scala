package org.pasalab.automj

import org.apache.spark.sql.MjContext
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-10.
 */
class MjContextSuite extends SharedSQLContext{
  test("strategy test") {
    val conf = spark.sparkContext.getConf
    conf.set(MjConfigConst.ONE_ROUND_STRATEGY, "org.pasalab.automj.ShareStrategy")
    conf.set(MjConfigConst.MULTI_ROUND_STRATEGY, "org.pasalab.automj.LeftDepthStrategy")
    conf.set(MjConfigConst.JOIN_SIZE_ESTIMATOR, "org.pasalab.automj.EstimatorBasedSample")
    val mjContext = new MjContext(spark)
    if (!mjContext.oneRoundStrategy.isInstanceOf[ShareStrategy]) {
      fail("one round strategy is not specific class")
    }
    if (!mjContext.multiRoundStrategy.isInstanceOf[LeftDepthStrategy]) {
      fail("multi round strategy is not specific class")
    }
    if (!mjContext.joinSizeEstimator.isInstanceOf[EstimatorBasedSample]) {
      fail("join size estimator is not specific class")
    }
  }
}
