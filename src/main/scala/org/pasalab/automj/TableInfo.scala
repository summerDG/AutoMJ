package org.pasalab.automj

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class TableInfo(name: String, size: Long, count: Long, cardinality: Map[String, Long], sample: DataFrame) {
  def getCardinality(field: String): Option[Long] = cardinality.get(field)
  def getCardinality(expr: Expression): Option[Long] = expr match {
    case a: AttributeReference =>
      cardinality.get(a.name)
  }
}
