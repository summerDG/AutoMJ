package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.MjOptimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.pasalab.automj.{Catalog, MetaManager, MjConfigConst, ReduceTreeStrategy}

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
  val reduceStrategy: ReduceTreeStrategy = Class.forName(conf.get(MjConfigConst.REDUCE_TREE_STRATEGY))
    .getConstructor(classOf[MetaManager]).newInstance(meta).asInstanceOf[ReduceTreeStrategy]

  session.experimental.extraOptimizations ++= Seq[Rule[LogicalPlan]](MjOptimizer(reduceStrategy))
  session.experimental.extraStrategies ++= Seq[Strategy]()
}
