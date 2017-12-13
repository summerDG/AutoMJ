package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.command.CreateViewCommand


/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiRoundSuite extends QueryTest with SharedMjContext with ArgumentsSet{
  test("left depth optimize method") {
    val dataSource = lineData
    dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))
    val multiRoundStrategy: MultiRoundStrategy = new LeftDepthStrategy(meta)
    val plan = multiRoundStrategy.optimize(dataSource.joinConditions, dataSource.relations)

    assert(plan.isInstanceOf[Join], "Not Join Node")

    val tableName= Seq[String]("a", "b", "c", "d")

    plan match {
      case j: Join =>
        val left = j.left
        val right = j.right
        right match {
          case c: CreateViewCommand =>
            c.name.table match {
              case "a" =>
                aIsEnd(left, 1, tableName)
              case "d" =>
                dIsEnd(left, 2, tableName)
              case x =>
                fail(s"Last Join relation should be a or d (not $x)")
            }
          case _ => fail(s"should be CreateViewCommand (${right.getClass.getName})")
        }
    }
  }
  private def aIsEnd(head: LogicalPlan, idx: Int, tableName: Seq[String]): Unit = {
    val exp = tableName(idx)
    head match {
      case join: Join =>
        val left = join.left
        val right = join.right
        right match {
          case r: CreateViewCommand =>
            val t: String = r.name.table
            assert(t == exp, s"If the query is end with a, the next Join table is $exp (not $t)")
            aIsEnd(left, idx+1, tableName)
          case _ => fail(s"should be CreateViewCommand (${right.getClass.getName})")
        }
      case c: CreateViewCommand =>
        val t: String = c.name.table
        assert(idx == tableName.length - 1 && t == exp, s"first table is ${tableName.last}(not $t-$idx)")
    }
  }
  private def dIsEnd(head: LogicalPlan, idx: Int, tableName: Seq[String]): Unit = {
    val exp = tableName(idx)
    head match {
      case join: Join =>
        val left = join.left
        val right = join.right
        right match {
          case r: CreateViewCommand =>
            val t: String = r.name.table
            assert(t == exp, s"If the query is end with d, the next Join table is $exp (not $t)")
            dIsEnd(left, idx-1, tableName)
          case _ => fail(s"should be CreateViewCommand (${right.getClass.getName})")
        }
      case c: CreateViewCommand =>
        val t: String = c.name.table
        assert(idx == 0 && t == exp, s"first table is ${tableName.head}(not $t-$idx)")
    }
  }
}
