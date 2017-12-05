package org.pasalab.automj

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SQLContext, Statistics}

import scala.collection.mutable
/**
 * Created by wuxiaoqi on 17-11-28.
 */
class MetaManager(catalog: Catalog, sqlContext: SQLContext) {
  val addedInfo: mutable.ArrayBuffer[TableInfo] = mutable.ArrayBuffer[TableInfo]()
  def getInfo(tableName: String):Option[TableInfo] = catalog.getTable(tableName)
  private def registerTable(tableName: String,
                    size: Long,
                    count: Long,
                    cardinality: Map[String, Long],
                    sample: DataFrame): Unit ={
    catalog.registerTable(tableName, TableInfo(size, count, cardinality, sample))
  }
  def registerTable(tableName: String, dataFrame: DataFrame, fraction: Double): Unit ={
    val statistics = new Statistics(dataFrame, sqlContext, fraction)
    registerTable(tableName, statistics.getSize, statistics.getCount, statistics.getCardinality, statistics.getSample)
  }
  //TODO: LogicalPlan -> String -> TableInfo
  def getInfo(plan: LogicalPlan): Option[TableInfo] = None
}
