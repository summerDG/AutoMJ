package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.automj.{MjSessionCatalog, MjSessionStateBuilder}
import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.execution.ShareJoinSelection
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.pasalab.automj.{JoinSizeEstimator, MjConfigConst, MultiRoundStrategy, OneRoundStrategy}

/**
 * Created by wuxiaoqi on 17-12-14.
 */
//TODO: 在完善的时候还是要建立Builder, 因为可能存在多session的问题
class MjSession private(
                         @transient override val sparkContext: SparkContext,
                         @transient private val existingSharedState: Option[SharedState],
                         @transient private val parentSessionState: Option[SessionState],
                         @transient override private[sql] val extensions: SparkSessionExtensions)
  extends SparkSession(sparkContext) { self =>
  def this(sc: SparkContext) {
    this(sc, None, None, MjSession.newExtensions())
  }
  @InterfaceStability.Unstable
  @transient
  override lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val configs = sparkContext.conf.getAll
        val state = MjSession.instantiateSessionState(self)
        configs.foreach{case (k,v) => state.conf.setConfString(k, v)}
        state
      }
  }

  @InterfaceStability.Unstable
  @transient
  override lazy val sharedState: SharedState = {
    existingSharedState.getOrElse {
//      if (sparkContext.getConf.getBoolean(MjConfigConst.ENABLE_STATISTICS, false))
//        new MjSharedState(sparkContext)
//      else
//        new SharedState(sparkContext)
      new SharedState(sparkContext)
    }
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
object MjSession {
  private def instantiateSessionState(sparkSession: SparkSession): SessionState = {
    new MjSessionStateBuilder(sparkSession, None).build()
  }
  def newExtensions(): SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    // 增添新逻辑优化策略
    extensions.injectOptimizerRule {
      session =>
        val sqlConf = session.sqlContext.conf
        val sparkConf = session.sparkContext.conf
        val catalog: MjSessionCatalog = session.sessionState.catalog.asInstanceOf[MjSessionCatalog]
        val multiRoundStrategy: Option[MultiRoundStrategy] = sparkConf.getOption(MjConfigConst.MULTI_ROUND_STRATEGY)
          .map(c => Class.forName(c).getConstructors.head.newInstance(sqlConf).asInstanceOf[MultiRoundStrategy])

        val oneRoundStrategy: Option[OneRoundStrategy] = sparkConf.getOption(MjConfigConst.ONE_ROUND_STRATEGY)
          .map(c =>Class.forName(c).getConstructors.head.newInstance(catalog, sqlConf).asInstanceOf[OneRoundStrategy])

        val joinSizeEstimator: Option[JoinSizeEstimator] = sparkConf.getOption(MjConfigConst.JOIN_SIZE_ESTIMATOR)
          .map(c => Class.forName(c).getConstructors.head.newInstance(catalog, sqlConf).asInstanceOf[JoinSizeEstimator])

        val optimizer: MjOptimizer = MjOptimizer(oneRoundStrategy,
          multiRoundStrategy, joinSizeEstimator, sparkConf)
        optimizer
    }
    // 增添新物理优化策略
    extensions.injectPlannerStrategy {
      session =>
        val sparkConf = session.sqlContext.conf
        ShareJoinSelection(sparkConf)
    }
    extensions
  }
}
