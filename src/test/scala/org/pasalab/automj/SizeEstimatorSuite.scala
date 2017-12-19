package org.pasalab.automj

import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-12.
 */
class SizeEstimatorSuite extends SharedSQLContext{
  test("test cost model") {
    val dataSource = triangleData
    val expectedSize = 12
    val estimator: JoinSizeEstimator =
      EstimatorBasedSample(spark.sessionState.catalog.asInstanceOf[MjSessionCatalog], spark.sparkContext.getConf)
    estimator.refresh(dataSource.joinConditions, dataSource.relations)

    val cost: Long = estimator.cost()
    assert(cost == expectedSize, s"Estimated size is $cost, not $expectedSize")
  }
}
