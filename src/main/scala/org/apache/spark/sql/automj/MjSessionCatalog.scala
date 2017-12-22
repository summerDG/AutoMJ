package org.apache.spark.sql.automj

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Statistics => _, _}
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj.MjConfigConst

/**
 * Created by wuxiaoqi on 17-12-14.
 */
class MjSessionCatalog(externalCatalog: ExternalCatalog,
                       globalTempViewManager: GlobalTempViewManager,
                       functionRegistry: FunctionRegistry,
                       conf: SQLConf,
                       hadoopConf: Configuration,
                       parser: ParserInterface,
                       functionResourceLoader: FunctionResourceLoader,
                       sqlContext: SQLContext)
  extends SessionCatalog (externalCatalog,
    globalTempViewManager, functionRegistry, conf, hadoopConf, parser, functionResourceLoader){

  private val fraction: Double = conf.getConfString(MjConfigConst.SAMPLE_FRACTION).toDouble

  override def createGlobalTempView(name: String, viewDefinition: LogicalPlan, overrideIfExists: Boolean): Unit = {
    super.createGlobalTempView(name, viewDefinition, overrideIfExists)
    val view: Option[LogicalPlan] = getGlobalTempView(name)
    assert(view.isDefined, "no view after createGlobalTempView")
    addStatistics(view.get)
  }

  override def createTempView(name: String, tableDefinition: LogicalPlan, overrideIfExists: Boolean): Unit = {
    super.createTempView(name, tableDefinition, overrideIfExists)
    val view: Option[LogicalPlan] = getTempView(name)
    assert(view.isDefined, "no view after createTempView")
    addStatistics(view.get)
  }
  // 使用反射给View对应的LogicalPlan增加统计信息
  def addStatistics(plan: LogicalPlan): Unit = {
    val statsField = plan.getClass.getDeclaredField("statsCache")
    statsField.setAccessible(true)
    statsField.set(plan, Some(StatisticsUtils.generateCatalogStatistics(sqlContext.sparkSession, plan, fraction)))
  }
}
