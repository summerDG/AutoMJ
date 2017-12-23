package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiJoinSuite extends QueryTest with SharedSQLContext{
  import testImplicits._
  setupTestData()
  //TODO: 改正sql String
  test("triangle data correctness test(one round)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true"){
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("triangle data correctness test(use cost model)") {
    withSQLConf(
      MjConfigConst.JOIN_DEFAULT_SIZE -> "72") {
      checkAnswer(sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z"), expectedTriangle())
    }
  }
  test("clique data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "144") {
      checkAnswer(
        sql("SELECT * FROM a, b, c, d, e, f where a.x = b.x AND b.y = c.y AND c.z = a.z AND d.s = e.s AND e.s =f.s AND d.z = c.z AND e.x = a.x AND f.y = b.y"),
        expectedClique())
    }
  }
  test("line data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "12") {
      checkAnswer(sql("SELECT * FROM al, b, c, dl where al.x = b.x AND b.y = c.y AND c.z = dl.z"), expectedLine())
    }
  }
  test("arbitrary data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "72") {
      checkAnswer(sql("SELECT * FROM a, b, c , da, ea, fa, g, h, i, j " +
        "where a.x = b.x AND b.y = c.y AND c.z = a.z AND a.z = da.z AND da.p = ea.p AND ea.q = fa.q AND fa.s = g.s " +
        "AND b.y = h.y AND h.r = i.r AND i.w = j.w"), expectedArbitrary())
    }
  }
}
