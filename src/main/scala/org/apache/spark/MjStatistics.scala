package org.apache.spark

import java.math.{MathContext, RoundingMode}
import java.util.UUID

import joinery.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, HintInfo, Statistics}
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * Created by wuxiaoqi on 17-12-22.
 */
case class MjStatistics(
                       sizeInBytes: BigInt,
                       rowCount: Option[BigInt],
                       attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),
                       hints: HintInfo = HintInfo(),
                       sample: DataFrame[Any], fraction: Double
                       ){
  override def toString: String = s"MjStatistics($simpleString)"

  def toStatistics: Statistics = {
    Statistics(sizeInBytes, rowCount, attributeStats, hints)
  }
  def extractSample: SampleStat[Any] = SampleStat(sample, fraction)

  def simpleString: String = {
    Seq(s"sizeInBytes=${Utils.bytesToString(sizeInBytes)}",
      if (rowCount.isDefined) {
        s"rowCount=${BigDecimal(rowCount.get, new MathContext(3, RoundingMode.HALF_UP)).toString()}"
      } else {
        ""
      },
      s"hints=$hints", s"sample(${sample.toString(100)}) by $fraction").filter(_.nonEmpty).mkString(", ")
  }
}
case class SampleStat[T](sample: DataFrame[T], fraction: Double)
