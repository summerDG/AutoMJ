package org.pasalab.automj

import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-12.
 */
class SizeEstimatorSuite extends SharedSQLContext{
  test("test cost model") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "36") {
      val dataSource = triangleData
      val expectedSize = 36
      val estimator: JoinSizeEstimator =
        EstimatorBasedSample(catalog, sqlConf)
      estimator.refresh(dataSource.joinConditions, dataSource.relations)

      val cost: BigInt = estimator.cost()
      assert(cost == expectedSize, s"Estimated size is $cost, not $expectedSize")
    }
  }
}
