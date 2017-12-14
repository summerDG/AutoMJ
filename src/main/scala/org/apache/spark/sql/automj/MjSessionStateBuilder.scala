package org.apache.spark.sql.automj

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{MjOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.execution.{ShareJoinSelection, SparkPlanner}
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-12-14.
 */
class MjSessionStateBuilder(session: SparkSession, parentState: Option[SessionState])
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = new MjSessionStateBuilder(_, _)

  override protected lazy val catalog: MjSessionCatalog = {
    val catalog = new MjSessionCatalog(
      session.sharedState.externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader,
      session.sqlContext,
      session.read,
      session.sparkContext.conf.getOption(MjConfigConst.METADATA_LOCATION)
    )
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) {

      override def extraPlanningStrategies: Seq[Strategy] = {
        val shareJoinStrategy: Strategy = ShareJoinSelection(catalog, conf)

        super.extraPlanningStrategies ++ Seq[Strategy](shareJoinStrategy)
      }
    }
  }

  override protected def optimizer: Optimizer = {
    new Optimizer(session.sessionState.catalog, conf) {
      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = {
        val sparkConf = session.sparkContext.conf
        val multiRoundStrategy: Option[MultiRoundStrategy] = sparkConf.getOption(MjConfigConst.MULTI_ROUND_STRATEGY)
          .map(c => Class.forName(c).getConstructor(classOf[MjSessionCatalog]).newInstance(catalog).asInstanceOf[MultiRoundStrategy])
        val oneRoundStrategy: Option[OneRoundStrategy] = sparkConf.getOption(MjConfigConst.ONE_ROUND_STRATEGY)
          .map(c =>Class.forName(c).getConstructor(classOf[MjSessionCatalog]).newInstance(catalog).asInstanceOf[OneRoundStrategy])
        val joinSizeEstimator: Option[JoinSizeEstimator] = sparkConf.getOption(MjConfigConst.JOIN_SIZE_ESTIMATOR)
          .map(c => Class.forName(c).getConstructor(classOf[MjSessionCatalog], classOf[SparkConf])
            .newInstance(catalog, sparkConf).asInstanceOf[JoinSizeEstimator])

        val optimizer: MjOptimizer = MjOptimizer(oneRoundStrategy,
          multiRoundStrategy, joinSizeEstimator, sparkConf.getBoolean(MjConfigConst.Force_ONE_ROUND, false))

        super.extendedOperatorOptimizationRules ++ Seq[Rule[LogicalPlan]](optimizer)
      }
    }
  }
}
