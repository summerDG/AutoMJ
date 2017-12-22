package org.pasalab.automj

import joinery.DataFrame
import org.apache.spark.{MjStatistics, SparkConf}
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class JoinSizeEstimator(conf: SQLConf) {
  protected var p: Double = 1
  protected var joinConditions: Map[(Int, Int), Column] = null
  protected var relations: Seq[LogicalPlan] = null
  def refresh(joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan]): Unit = {
    joinConditions = joins.map {
      case (k, (lk, rk)) =>
        val expr = lk.zip(rk).map{
          case (l, r) =>
            EqualTo(l, r).asInstanceOf[Expression]
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

  def getProbability(): Seq[Double] = {
    val p = relations.map(r => r.stats(conf).asInstanceOf[MjStatistics[Any]].fraction)
    assert(relations.length == p.length, "some relations have no sample probability")
    p
  }
  def getSamples(): Seq[DataFrame[Any]] = {
    val s = relations.map(r => r.stats(conf).asInstanceOf[MjStatistics[Any]].sample)
    assert(relations.length == s.length, "some relations have no sample")
    s
  }
}
