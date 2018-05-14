package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiJoinSuite extends QueryTest with SharedSQLContext{
  setupTestData()
  test("triangle data correctness test(one round)") {
    withSQLConf(MjConfigConst.Force_ONE_ROUND -> "true",
      MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.EXECUTION_MODE -> "one-round",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8"){
      val frame = sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z")
      checkAnswer(frame, expectedTriangle())
    }
  }
  test("triangle data correctness test(use cost model)") {
    withSQLConf(
      MjConfigConst.JOIN_DEFAULT_SIZE -> "40",
      MjConfigConst.EXECUTION_MODE -> "mixed-conf",
      MjConfigConst.JOIN_TREE -> "{[x,z,x,y,y,z]|Share|12{[x,z]|Join|12}{[x,y]|Join|12}{[y,z]|Join|12}}",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8") {
      val frame = sql("SELECT * FROM a, b, c where a.x = b.x AND b.y = c.y AND c.z = a.z")
      //assert(false, s"${frame.queryExecution.optimizedPlan}\n${frame.queryExecution.sparkPlan}\n${frame.queryExecution.executedPlan}")
      checkAnswer(frame, expectedTriangle())
    }
  }
  test("clique data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.EXECUTION_MODE -> "mixed-conf",
      MjConfigConst.JOIN_TREE -> "{[x,z,x,y,y,z,s,z,s,x,s,y]|Share|12{[x,z]|Join|12}{[x,y]|Join|12}{[y,z]|Join|12}{[s,z]|Join|12}{[s,x]|Join|12}{[s,y]|Join|12}}",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "16") {
      checkAnswer(
        sql("SELECT * FROM a, b, c, d, e, f where a.x = b.x AND b.y = c.y AND c.z = a.z AND d.s = e.s AND e.s =f.s AND d.z = c.z AND e.x = a.x AND f.y = b.y"),
        expectedClique())
    }
  }
  test("line data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "12",
      MjConfigConst.EXECUTION_MODE -> "mixed-conf",
      MjConfigConst.JOIN_TREE -> "{[x,x,y,y,z,z]|Join|12{[x,x,y]|Join|12{[x]|Join|12}{[x,y]|Join|12}}{[y,z]|Join|12}{[z]|Join|12}}",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8") {
      checkAnswer(sql("SELECT * FROM al, b, c, dl where al.x = b.x AND b.y = c.y AND c.z = dl.z"), expectedLine())
    }
  }
  test("conf tree correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "12",
      MjConfigConst.EXECUTION_MODE -> "mixed-conf",
      MjConfigConst.JOIN_TREE -> "{[p,p,p,q,q,s,s,x,x,y,y,z,z,z]|Join|100000{[p,x,x,y,y,z,z,z]|Join|125120000{[x,x,y,y,z,z]|Share|3536000{[x,y]|Join|798707}{[y,z]|Join|798707}{[z,x]|Join|969428}}{[p,z]|Join|798707}}{[p,p,q,q,s,s]|Share|3536000{[p,q]|Join|798707}{[q,s]|Join|798707}{[p,s]|Join|969428}}}",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8") {
      checkAnswer(sql("SELECT * FROM a, b, c, da, fa, ea, ga WHERE b.y = c.y AND c.z = a.z AND a.x = b.x AND ea.q = fa.q AND fa.s = ga.s AND ga.p = ea.p AND da.z = c.z AND da.p = ea.p"), expectedLine())
    }
  }
  test("arbitrary data correctness test") {
    withSQLConf(MjConfigConst.JOIN_DEFAULT_SIZE -> "1000",
      MjConfigConst.EXECUTION_MODE -> "mixed-fix",
      MjConfigConst.ONE_ROUND_ONCE -> "true",
      MjConfigConst.ONE_ROUND_PARTITIONS -> "8") {
      checkAnswer(sql("SELECT * FROM a, b, c , da, ea, fa, g, h, i, j " +
        "where a.x = b.x AND b.y = c.y AND c.z = a.z AND a.z = da.z AND da.p = ea.p AND ea.q = fa.q AND fa.s = g.s " +
        "AND b.y = h.y AND h.r = i.r AND i.w = j.w"), expectedArbitrary())
    }
  }
}
