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
    val marked: mutable.Set[Int] = mutable.Set[Int]()
    val edges: Map[Int, Seq[Int]] = joinConditions.toSeq.map(_._1).flatMap {
      case (l, r) =>
        Seq[(Int, Int)]((l, r), (r, l))
    }.groupBy(_._1).map {
      case (k, v) =>
        (k, v.map(_._2))
    }

    val firstId = edges.filter(p => p._2.length == 1).head._1
    var currentVertices:Array[Int] = Array[Int](firstId)

    var join: LogicalPlan = relations(firstId)

    def generateJoin(v: Int, n: Int, plan: LogicalPlan): Unit = {
      val (left, right) = joinConditions((v, n))
      val condition: Expression = left.zip(right).map {
        case (l, r) => EqualTo(l, r).asInstanceOf[Expression]
      }.reduce((l, r) => And(l, r))
      join = Join(join, plan, Inner, Some(condition))
    }

    var joinSeq = s"$firstId"
    while (!currentVertices.isEmpty) {
      val newVertices: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer[Int]()

      for (v <- currentVertices) {
        marked.add(v)
        val neighbourhood = edges(v).filter(x => !marked.contains(x))

        for (n <- neighbourhood) {
          assert(joinConditions.contains((v, n))|| joinConditions.contains((n, v)), "construct graph problem")
          joinSeq += s", $n"
          if (joinConditions.contains((v, n))) {
            generateJoin(v, n, relations(n))
          } else {
            generateJoin(n, v, relations(n))
          }
        }
        newVertices ++= neighbourhood
      }

      currentVertices = newVertices.toArray
    }
    join
  }
}
