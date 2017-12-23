package org.pasalab.automj

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.test.SharedSQLContext


/**
 * Created by wuxiaoqi on 17-12-13.
 */
class MultiRoundSuite extends QueryTest with SharedSQLContext{
  test("left depth optimize method") {
    val dataSource = lineData
    val multiRoundStrategy: MultiRoundStrategy = new LeftDepthStrategy(sqlConf)
    val plan = multiRoundStrategy.optimize(dataSource.joinConditions, dataSource.relations)

    assert(plan.isInstanceOf[Join], "Not Join Node")

    val tableName= Seq[String]("a", "b", "c", "d")

    plan match {
      case j: Join =>
        val left = j.left
        val right = j.right
        val name = getTableNameFromPlan(right)
        helper(name, left)
    }
    def helper(s: String, left: LogicalPlan): Unit = s match {
      case "a" =>
        aIsEnd(left, 1, tableName)
      case "d" =>
        dIsEnd(left, 2, tableName)
      case x =>
        fail(s"Last Join relation should be a or d (not $x, left is join: ${left.isInstanceOf[Join]})")
    }
  }

  def getTableNameFromPlan(c: LogicalPlan): String = {
    val output = c.output
    if (output.length == 1) {
      if (output.head.name == "x") "a" else "d"
    } else if (output.length == 2){
      val first = output(0).name
      val second = output(1).name
      (first, second) match {
        case ("x", "y") => "b"
        case ("y", "z") => "c"
      }
    } else {
      throw new IllegalArgumentException
    }
  }

  private def aIsEnd(head: LogicalPlan, idx: Int, tableName: Seq[String]): Unit = {
    val exp = tableName(idx)
    head match {
      case join: Join =>
        val left = join.left
        val right = join.right
        right match {
          case r =>
            val t: String = getTableNameFromPlan(r)
            assert(t == exp, s"If the query is end with a, the next Join table is $exp (not $t)")
            aIsEnd(left, idx+1, tableName)
        }
      case c =>
        val t: String = getTableNameFromPlan(c)
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
          case r =>
            val t: String = getTableNameFromPlan(r)
            assert(t == exp, s"If the query is end with d, the next Join table is $exp (not $t)")
            dIsEnd(left, idx-1, tableName)
          case _ => fail(s"should be CreateViewCommand (${right.getClass.getName})")
        }
      case c =>
        val t: String = getTableNameFromPlan(c)
        assert(idx == 0 && t == exp, s"first table is ${tableName.head}(not $t-$idx)")
    }
  }
}
