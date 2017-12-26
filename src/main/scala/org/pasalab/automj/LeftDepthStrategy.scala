package org.pasalab.automj
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-7.
 */
case class LeftDepthStrategy(conf: SQLConf) extends MultiRoundStrategy(conf){
  override def optimize(joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                        relations: Seq[LogicalPlan]): LogicalPlan = {
    // 用于标记已经用过的条件, 这个作用对于环形的查询来说是有必要的
    val marked: mutable.Set[(Int, Int)] = mutable.HashSet[(Int, Int)]()
    // 用于标记已经再语法树中的表
    val scanned: mutable.Set[Int] = mutable.HashSet[Int]()
    val edges: Map[Int, Seq[Int]] = joinConditions.toSeq.map(_._1).flatMap {
      case (l, r) =>
        Seq[(Int, Int)]((l, r), (r, l))
    }.groupBy(_._1).map {
      case (k, v) =>
        (k, v.map(_._2))
    }

    val firstId = {
      val endNode = edges.filter(p => p._2.length == 1)
      if (endNode.isEmpty) 0
      else endNode.map(_._1).min
    }
    var currentVertices:Array[Int] = Array[Int](firstId)

    var join: LogicalPlan = relations(firstId)

    def generateJoin(v: Int, n: Int, plan: LogicalPlan): Unit = {
      val (left, right) = joinConditions((v, n))
      val condition: Expression = left.zip(right).map {
        case (l, r) => EqualTo(l, r).asInstanceOf[Expression]
      }.reduce((l, r) => And(l, r))
      join = Join(join, plan, Inner, Some(condition))
    }

    while (!currentVertices.isEmpty) {
      val newVertices: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer[Int]()

      for (v <- currentVertices) {
        scanned.add(v)
        val neighbourhood = edges(v).filter{
          case x =>
            if  (v < x) {
              !marked(v, x)
            } else !marked(x, v)
        }

        for (n <- neighbourhood) {
          assert(joinConditions.contains((v, n))|| joinConditions.contains((n, v)), "construct graph problem")
          if (joinConditions.contains((v, n))) {
            generateJoin(v, n, relations(n))
          } else {
            generateJoin(n, v, relations(n))
          }
          if (v < n) marked.add((v, n))
          else marked.add((n, v))
        }
        newVertices ++= neighbourhood.filter(x => !scanned.contains(x))
      }

      currentVertices = newVertices.toArray
    }
    join
  }
}
