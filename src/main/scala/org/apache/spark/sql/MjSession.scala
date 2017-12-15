package org.apache.spark.sql

import java.io.Closeable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.automj.MjSessionStateBuilder
import org.apache.spark.sql.internal.{SessionState, SharedState}

/**
 * Created by wuxiaoqi on 17-12-14.
 */
class MjSession private(
                         @transient override val sparkContext: SparkContext,
                         @transient private val existingSharedState: Option[SharedState],
                         @transient private val parentSessionState: Option[SessionState],
                         @transient override private[sql] val extensions: SparkSessionExtensions)
  extends SparkSession(sparkContext) with Serializable with Closeable with Logging { self =>
  private[sql] def this(sc: SparkContext) {
    this(sc, None, None, new SparkSessionExtensions)
  }
  private val session = new SparkSession(sparkContext)
  override lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val confs = session.conf.getAll
        val state = new MjSessionStateBuilder(session, None).build()
        confs.foreach{case (k,v) => state.conf.setConfString(k, v)}
        state
      }
  }
}
