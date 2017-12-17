package org.apache.spark.sql

import java.io.Closeable

import org.apache.spark.SparkContext
import org.apache.spark.annotation.InterfaceStability
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
        val configs = session.conf.getAll
        val state = new MjSessionStateBuilder(session, None).build()
        configs.foreach{case (k,v) => state.conf.setConfString(k, v)}
        state
      }
  }

  @InterfaceStability.Unstable
  @transient
  override lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext))
  }
  override def newSession(): SparkSession = {
    new MjSession(sparkContext, Some(sharedState), parentSessionState = None, extensions)
  }

  override private[sql] def cloneSession(): SparkSession = {
    val result = new MjSession(sparkContext, Some(sharedState), Some(sessionState), extensions)
    result.sessionState // force copy of SessionState
    result
  }
}
