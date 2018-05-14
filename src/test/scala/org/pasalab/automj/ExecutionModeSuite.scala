package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 18-1-1.
 */
class ExecutionModeSuite extends QueryTest with SharedSQLContext{
  setupTestData()
  test("execution mode (default)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true",
      MjConfigConst.EXECUTION_MODE -> "default",
      MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8"){
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("execution mode (one-round)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true",
      MjConfigConst.EXECUTION_MODE -> "one-round",
      MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8"){
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("execution mode (mixed)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true",
      MjConfigConst.EXECUTION_MODE -> "mixed-fix",
      MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8"){
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
}
