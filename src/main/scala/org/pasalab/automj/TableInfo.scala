package org.pasalab.automj

import org.apache.spark.sql.DataFrame

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class TableInfo(size: Long, count: Long, cardinality: Map[String, Long], sample: DataFrame) {
  def getCardinality(field: String): Option[Long] = cardinality.get(field)
}
