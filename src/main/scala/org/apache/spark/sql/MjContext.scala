package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-11-27.
 */
class MjContext(val session: SparkSession) extends Serializable with Logging {
  val conf: SparkConf = session.sparkContext.getConf
  val sqlContext: SQLContext = session.sqlContext
  val meta: MetaManager = new MetaManager(
    new Catalog(conf.get(MjConfigConst.METADATA_LOCATION), session),
    sqlContext)

  // Rule需要传入meta, 从而可以获取各种表的元信息
  val multiRoundStrategy: MultiRoundStrategy = Class.forName(conf.get(MjConfigConst.MULTI_ROUND_STRATEGY))
    .getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[MultiRoundStrategy]
  val oneRoundStrategy: OneRoundStrategy = Class.forName(conf.get(MjConfigConst.ONE_ROUND_STRATEGY))
    .getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[OneRoundStrategy]
  val joinSizeEstimator: JoinSizeEstimator = Class.forName(conf.get(MjConfigConst.JOIN_SIZE_ESTIMATOR))
    .getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[JoinSizeEstimator]
  // 物理优化的策略，这里需要实现Attribute Order Optimization的接口
  val shareJoinStrategy: Strategy = Class.forName(conf.get(MjConfigConst.SHARE_JOIN_STRATEGY))
    .getConstructor(classOf[MetaManager], classOf[SQLConf])
    .newInstance(meta, session.sessionState.conf).asInstanceOf[Strategy]

  session.experimental.extraOptimizations ++= Seq[Rule[LogicalPlan]](
    MjOptimizer(oneRoundStrategy, multiRoundStrategy, joinSizeEstimator))

  session.experimental.extraStrategies ++= Seq[Strategy](shareJoinStrategy)
}
