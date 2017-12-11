package org.pasalab.automj

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.{DataFrame, SQLContext, Statistics}

/**
 * Created by wuxiaoqi on 17-11-28.
 */
class MetaManager(catalog: Catalog, sqlContext: SQLContext) {
  def getInfo(tableName: String):Option[TableInfo] = catalog.getTable(tableName)
  private def registerTable(tableName: String,
                    size: Long,
                    count: Long,
                    cardinality: Map[String, Long],
                    sample: DataFrame, p: Double): Unit ={
    catalog.registerTable(tableName, TableInfo(tableName, size, count, cardinality, sample, p))
  }
  def registerTable(tableName: String, dataFrame: DataFrame, p: Double, fraction: Double): Unit ={
    val statistics = new Statistics(dataFrame, sqlContext, fraction)
    registerTable(tableName, statistics.getSize, statistics.getCount, statistics.getCardinality, statistics.getSample, p)
  }
  //TODO: LogicalPlan -> String -> TableInfo
  //用alias作为表名
  def getInfo(plan: LogicalPlan): Option[TableInfo] = plan match {
    case a: SubqueryAlias =>
      catalog.getTable(a.alias)
  }
}
