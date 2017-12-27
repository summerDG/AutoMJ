package org.pasalab.automj
import joinery.DataFrame.JoinType
import org.apache.spark.SampleStat
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Add, And, EqualTo, Expression, NamedExpression}
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

    var join: LogicalPlan = null

    def generateJoin(r: Int): Unit = {
      val plan = relations(r)

      if (join == null) {
        join = plan
      } else {
        val conditions = scanned.flatMap {
          case l if (joinConditions.contains(l, r) || joinConditions.contains(r, l))=>
            if ((l < r && !marked.contains((l, r)) || (l >= r && !marked.contains(r, l)))) {
              if (l < r) marked.add((l, r))
              else marked.add(r, l)
              val (left, right) =
                if (joinConditions.contains((l, r))) {
                  joinConditions((l, r))
                } else {
                  val (a, b) = joinConditions((r, l))
                  (b, a)
                }
              Some(left.zip(right).map (x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce(And(_,_)))
            }
            else None
          case _ => None
        }
//        assert(r != 2, s"${conditions.mkString(",")}, left:${join}, right: ${plan}")
        assert(conditions.nonEmpty, s"conditions is empty, scanned: ${scanned.mkString(",")}, r: $r")
        join = Join(join, plan, Inner, Some(conditions.reduce(And(_,_))))
      }
    }

    while (!currentVertices.isEmpty) {
      val newVertices: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer[Int]()
      for (v <- currentVertices) {
        generateJoin(v)
        scanned += v
        val neighbourhood = edges(v).filter{
          case x =>
            !scanned.contains(x)
        }
        newVertices ++= neighbourhood
      }

      currentVertices = newVertices.filter(x => !scanned.contains(x)).toSet.toArray
    }
    join
  }
}
