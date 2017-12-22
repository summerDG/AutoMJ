package org.apache.spark

import java.math.{MathContext, RoundingMode}

import joinery.DataFrame
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, HintInfo, Statistics}
import org.apache.spark.util.Utils

/**
 * Created by wuxiaoqi on 17-12-22.
 */
case class MjStatistics[V](
                       override val sizeInBytes: BigInt,
                       override val rowCount: Option[BigInt],
                       override val attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),
                       override val hints: HintInfo = HintInfo(),
                       sample: DataFrame[V], fraction: Double
                       ) extends Statistics(sizeInBytes, rowCount, attributeStats, hints){
  override def toString: String = s"MjStatistics($simpleString)"

  override def simpleString: String = {
    Seq(s"sizeInBytes=${Utils.bytesToString(sizeInBytes)}",
      if (rowCount.isDefined) {
        s"rowCount=${BigDecimal(rowCount.get, new MathContext(3, RoundingMode.HALF_UP)).toString()}"
      } else {
        ""
      },
      s"hints=$hints", s"sample(${sample.toString(100)}) by $fraction")
  }
}
