package org.pasalab.automj

import joinery.DataFrame
import org.apache.spark.MjStatistics
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}


/**
 * Created by wuxiaoqi on 17-12-22.
 */
case class MjCatalogStatistics[V](override val sizeInBytes: BigInt,
                               override val rowCount: Option[BigInt] = None,
                               override val colStats: Map[String, ColumnStat],
                               sample: DataFrame[V])
  extends CatalogStatistics(sizeInBytes, rowCount, colStats){
  override def toPlanStats(planOutput: Seq[Attribute]): Statistics = {
    val matched = planOutput.flatMap(a => colStats.get(a.name).map(a -> _))
    MjStatistics(sizeInBytes = sizeInBytes, rowCount = rowCount, attributeStats = AttributeMap(matched), sample = sample)
  }

  override def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString, sample(${sample.toString(100)})"
  }
}
