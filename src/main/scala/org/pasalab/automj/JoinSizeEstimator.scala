package org.pasalab.automj

import joinery.DataFrame
import org.apache.spark.SampleStat
import org.apache.spark.sql.Column
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class JoinSizeEstimator(catalog: MjSessionCatalog, conf: SQLConf) {
  protected var p: Double = 1
  protected var joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = null
  protected var relations: Seq[LogicalPlan] = null
  def refresh(joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan]): Unit = {
    joinConditions = joins
    relations = tables
  }
  def joinSample(condition: Map[(Int, Int), Option[Expression]],
              samples: Seq[SampleStat[Any]]): SampleStat[Any]
  protected def costCore: BigInt

  def cost(): BigInt = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    costCore
  }
  def hasArgument: Boolean = {
    joinConditions != null && relations != null
  }

  def getProbability(): Seq[Double] = {
    val p = relations.flatMap(r => catalog.getSample(r)).map(_.fraction)
    assert(relations.length == p.length, "some relations have no sample probability")
    p
  }
  def getSamples(): Seq[DataFrame[Any]] = {
    val s = relations.flatMap(r => catalog.getSample(r)).map(_.sample)
    assert(relations.length == s.length, "some relations have no sample")
    s
  }
}
