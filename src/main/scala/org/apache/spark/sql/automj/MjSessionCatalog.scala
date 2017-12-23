package org.apache.spark.sql.automj

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SampleStat
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Statistics => _, _}
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj.MjConfigConst

import scala.collection.mutable

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
  private val samples = new mutable.HashMap[String, SampleStat[Any]]()

  override def createGlobalTempView(name: String, viewDefinition: LogicalPlan, overrideIfExists: Boolean): Unit = {
    super.createGlobalTempView(name, viewDefinition, overrideIfExists)
    val view: Option[LogicalPlan] = getGlobalTempView(name)
    assert(view.isDefined, "no view after createGlobalTempView")
    addStatistics(view.get, name)
  }

  override def createTempView(name: String, tableDefinition: LogicalPlan, overrideIfExists: Boolean): Unit = {
    super.createTempView(name, tableDefinition, overrideIfExists)
    val view: Option[LogicalPlan] = getTempView(name)
    assert(view.isDefined, "no view after createTempView")
    addStatistics(view.get, name)
  }
  // 使用反射给View对应的LogicalPlan增加统计信息
  def addStatistics(plan: LogicalPlan, name: String): Unit = {
    val viewName = formatTableName(name)
    val statsField = plan.getClass.getDeclaredField("statsCache")
    val accessible = statsField.isAccessible
    if (!accessible) statsField.setAccessible(true)
    val mjStatistics = StatisticsUtils.generateCatalogStatistics(sqlContext.sparkSession, viewName, plan, fraction)
    statsField.set(plan, Some(mjStatistics.toStatistics))
    statsField.setAccessible(accessible)

    samples += viewName -> mjStatistics.extractSample
  }

  def getSample(name: String): Option[SampleStat[Any]] = samples.get(formatTableName(name))

  def getSample(logicalPlan: LogicalPlan): Option[SampleStat[Any]] = {
    val tableName: Option[Any] = logicalPlan.stats(conf).attributeStats(StatisticsUtils.tableName).max
    assert(tableName.isDefined, "This logicalPlan has no tableName, so no Sample")
    getSample(tableName.get.asInstanceOf[String])
  }
}
