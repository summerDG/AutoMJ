package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.automj.MjSessionStateBuilder
import org.apache.spark.sql.internal.SessionState

/**
 * Created by wuxiaoqi on 17-12-14.
 */
class MjSession(sc: SparkContext) extends SparkSession(sc) { self =>
  override lazy val sessionState: SessionState = {
    super.sessionState
    new MjSessionStateBuilder(self, None).build()
  }
}