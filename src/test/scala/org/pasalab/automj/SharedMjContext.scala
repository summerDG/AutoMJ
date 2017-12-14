package org.pasalab.automj

import org.apache.spark.sql.MjContext
import org.apache.spark.sql.test.SharedSQLContext

import scala.util.Try

/**
 * Created by wuxiaoqi on 17-12-9.
 */
trait SharedMjContext extends SharedSQLContext{
  val mjContext = new MjContext(spark)
  val meta = mjContext.meta

  def withMjConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)

    (keys, values).zipped.foreach(spark.conf.set)

    mjContext.refresh()

    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }
}
