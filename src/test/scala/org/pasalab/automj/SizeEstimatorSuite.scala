package org.pasalab.automj

/**
 * Created by wuxiaoqi on 17-12-12.
 */
class SizeEstimatorSuite extends SharedMjContext with ArgumentsSet{
  test("test cost model") {
    val dataSource = triangleData
    val expectedSize = 8
    val estimator: JoinSizeEstimator = EstimatorBasedSample(meta, spark.sparkContext.getConf)
    estimator.refresh(dataSource.joinConditions, dataSource.relations)

    val cost: Long = estimator.cost()
    assert(cost == expectedSize, s"Estimated size is $cost, not 8")
  }
}
