package org.pasalab.automj

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class JoinSizeEstimator(meta: MetaManager, conf: SparkConf) {
  protected var p: Double = 1
  protected var joinConditions: Map[(Int, Int), Column] = null
  protected var relations: Seq[LogicalPlan] = null
  def refresh(joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan]): Unit = {
    joinConditions = joins.map {
      case (k, (lk, rk)) =>
        val expr = lk.zip(rk).map{
          case (l, r) =>
            EqualTo(l, r)
        }.reduce((a, b) => And(a, b))
        (k, new Column(expr))
    }
    relations = tables
  }
  protected def costCore: Long

  def cost(): Long = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    costCore
  }
  def hasArgument: Boolean = {
    joinConditions != null && relations != null
  }

  def getSamples(): Seq[DataFrame] = {
    val s = relations.flatMap (l => meta.getInfo(l)).map(_.sample)
    assert(relations.length == s.length, "some relations have no sample")
    s
  }
}
