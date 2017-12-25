package org.apache.spark.sql.automj

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{MjStatistics, SampleStat}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
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
  private val tempStatistics = new mutable.HashMap[String, MjStatistics]()
  private val statsName: String = "org$apache$spark$sql$catalyst$plans$logical$LogicalPlan$$statsCache"
  private val statsField = classOf[LogicalPlan].getDeclaredField(statsName)
  private val accessible = statsField.isAccessible

  if (!accessible) statsField.setAccessible(true)

  var code: Int = 1

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
    val mjStatistics = StatisticsUtils.generateCatalogStatistics(sqlContext.sparkSession, code, plan, fraction)
    assert(mjStatistics.rowCount.isDefined, s"$viewName rowCount is None")
    assert(mjStatistics.attributeStats.nonEmpty, s"$viewName attributeStats is empty")
    tempStatistics += formatAttributeName(plan.output.map(_.qualifiedName)) -> mjStatistics
//    assert(code < Int.MaxValue && code >= 0, s"code: $code")
//    statsField.set(plan, Some(Statistics(BigInt(code))))

//    samples += viewName -> mjStatistics.extractSample
//    code += 1
  }
  def formatAttributeName(attrs: Seq[String]): String = {
    attrs.sorted.reduce(_ + "," + _)
  }

  def getSample(code: String): Option[SampleStat[Any]] = {
//    if (tempStatistics.length > code)
//      Some(tempStatistics(code).extractSample)
//    else None
    tempStatistics.get(code).map (_.extractSample)
  }

  def getSample(logicalPlan: LogicalPlan): Option[SampleStat[Any]] = {
//    val code: BigInt = logicalPlan.stats(conf).sizeInBytes
//    getSample(code.toInt)
    getSample(formatAttributeName(logicalPlan.output.map(_.qualifiedName)))
  }

  def getStatistics(code: String): Option[MjStatistics] = {
//    if (tempStatistics.length > code)
//      Some(tempStatistics(code))
//    else None
    assert(tempStatistics.contains(code), s"don't contain $code")
    tempStatistics.get(code)
  }
  def getStatistics(logicalPlan: LogicalPlan): Option[MjStatistics] = {
//    val code: BigInt = logicalPlan.stats(conf).sizeInBytes
//    assert(code<Int.MaxValue, s"id: $code, type: ${logicalPlan.getClass.getName}")
    getStatistics(formatAttributeName(logicalPlan.output.map(_.qualifiedName)))
  }
}
