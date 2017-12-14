package org.pasalab.automj

import org.apache.spark.sql.QueryTest

/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiJoinSuite extends QueryTest with SharedMjContext with ArgumentsSet {
  //TODO: 改正sql String
  test("triangle data correctness test(one round)") {
    withMjConf(MjConfigConst.Force_ONE_ROUND -> "true",
    MjConfigConst.ONE_ROUND_STRATEGY -> "org.pasalab.automj.ShareStrategy",
    MjConfigConst.MULTI_ROUND_STRATEGY -> "org.pasalab.automj.LeftDepthStrategy",
    MjConfigConst.JOIN_SIZE_ESTIMATOR -> "org.pasalab.automj.EstimatorBasedSample"){
      val dataSource = triangleData
      dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("triangle data correctness test(use cost model)") {
    withMjConf(
    MjConfigConst.ONE_ROUND_STRATEGY -> "org.pasalab.automj.ShareStrategy",
    MjConfigConst.MULTI_ROUND_STRATEGY -> "org.pasalab.automj.LeftDepthStrategy",
    MjConfigConst.JOIN_SIZE_ESTIMATOR -> "org.pasalab.automj.EstimatorBasedSample"){
      val dataSource = triangleData
      dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("clique data correctness test") {
    withMjConf(
    MjConfigConst.ONE_ROUND_STRATEGY -> "org.pasalab.automj.ShareStrategy",
    MjConfigConst.MULTI_ROUND_STRATEGY -> "org.pasalab.automj.LeftDepthStrategy",
    MjConfigConst.JOIN_SIZE_ESTIMATOR -> "org.pasalab.automj.EstimatorBasedSample"){
      val dataSource = triangleData
      dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedClique())
    }
  }
  test("line data correctness test") {
    withMjConf(
    MjConfigConst.ONE_ROUND_STRATEGY -> "org.pasalab.automj.ShareStrategy",
    MjConfigConst.MULTI_ROUND_STRATEGY -> "org.pasalab.automj.LeftDepthStrategy",
    MjConfigConst.JOIN_SIZE_ESTIMATOR -> "org.pasalab.automj.EstimatorBasedSample"){
      val dataSource = triangleData
      dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedLine())
    }
  }
  test("arbitrary data correctness test") {
    withMjConf(
    MjConfigConst.ONE_ROUND_STRATEGY -> "org.pasalab.automj.ShareStrategy",
    MjConfigConst.MULTI_ROUND_STRATEGY -> "org.pasalab.automj.LeftDepthStrategy",
    MjConfigConst.JOIN_SIZE_ESTIMATOR -> "org.pasalab.automj.EstimatorBasedSample"){
      val dataSource = triangleData
      dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedArbitrary())
    }
  }
}
