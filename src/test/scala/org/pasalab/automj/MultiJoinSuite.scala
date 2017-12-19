package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiJoinSuite extends QueryTest with SharedSQLContext{
  //TODO: 改正sql String
  test("triangle data correctness test(one round)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true"){
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("triangle data correctness test(use cost model)") {
    checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
  }
  test("clique data correctness test") {
    checkAnswer(
      sql("SELECT * FROM a, b, c, d, e, f where a.x = b.x AND b.y = c.y AND c.z = a.z AND d.s = e.s AND e.s =f.s AND d.z = c.z AND e.x = a.x AND f.y = b.y"),
      expectedClique())
  }
  test("line data correctness test") {
    checkAnswer(sql("SELECT * FROM a, b, c, d where a.x = b.x AND b.y = c.y AND c.z = d.z"), expectedLine())
  }
  test("arbitrary data correctness test") {
    checkAnswer(sql("SELECT * FROM a, b, c , d, e, f, g, h, i, j " +
      "where a.x = b.x AND b.y = c.y AND c.z = a.z AND a.z = d.z AND d.p = e.p AND e.q = f.q AND f.s = g.s " +
      "AND b.y = h.y AND h.r = i.r AND i.w = j.w"), expectedArbitrary())
  }
}
