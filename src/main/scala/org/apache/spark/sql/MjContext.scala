package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ShareJoinSelection
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-11-27.
 */
class MjContext(val session: SparkSession) extends Serializable with Logging {
  val conf: SparkConf = session.sparkContext.getConf
  val sqlContext: SQLContext = session.sqlContext
  val meta: MetaManager = new MetaManager(
    new Catalog(conf.getOption(MjConfigConst.METADATA_LOCATION), session),
    sqlContext)

  // Rule需要传入meta, 从而可以获取各种表的元信息
  val multiRoundStrategy: Option[MultiRoundStrategy] = conf.getOption(MjConfigConst.MULTI_ROUND_STRATEGY)
    .map(c => Class.forName(c).getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[MultiRoundStrategy])
  val oneRoundStrategy: Option[OneRoundStrategy] = conf.getOption(MjConfigConst.ONE_ROUND_STRATEGY)
    .map(c =>Class.forName(c).getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[OneRoundStrategy])
  val joinSizeEstimator: Option[JoinSizeEstimator] = conf.getOption(MjConfigConst.JOIN_SIZE_ESTIMATOR)
    .map(c => Class.forName(c).getConstructor(classOf[MetaManager], classOf[SparkConf])
      .newInstance(meta, conf).asInstanceOf[JoinSizeEstimator])

  val shareJoinStrategy: Strategy = ShareJoinSelection(meta, sqlContext.conf)

  private val optimizer: MjOptimizer = MjOptimizer(oneRoundStrategy,
    multiRoundStrategy, joinSizeEstimator, conf.getBoolean(MjConfigConst.Force_ONE_ROUND, false))
  session.experimental.extraOptimizations ++= Seq[Rule[LogicalPlan]](optimizer)

  session.experimental.extraStrategies ++= Seq[Strategy](shareJoinStrategy)
}
