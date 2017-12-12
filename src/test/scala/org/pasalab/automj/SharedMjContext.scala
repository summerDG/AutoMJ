package org.pasalab.automj

import org.apache.spark.sql.MjContext
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Created by wuxiaoqi on 17-12-9.
 */
trait SharedMjContext extends SharedSQLContext{
  val mjContext = new MjContext(spark)
  val meta = mjContext.meta
}
