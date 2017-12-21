package org.apache.spark.sql.automj

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Statistics => _, _}
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj.TableInfo

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

  val globalViewName: mutable.HashMap[LogicalPlan, String] = new mutable.HashMap[LogicalPlan, String]()

  override def createTempView(name: String,
                              tableDefinition: LogicalPlan,
                              overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempTables.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempTables.put(table, tableDefinition)
  }

  val globalTableInfo: mutable.HashMap[String, TableInfo] = new mutable.HashMap[String, TableInfo]()

  val tempTableInfo: mutable.HashMap[String, TableInfo] = new mutable.HashMap[String, TableInfo]()

  def getInfo(table: String): Option[TableInfo] = {
    if (tempTableInfo.contains(table)) tempTableInfo.get(table)
    else globalTableInfo.get(table)
  }


  def getInfo(plan: LogicalPlan): Option[TableInfo] = {
    //TODO: 能否用equals?
    val tempView = tempTables.filter(_._2.equals(plan))
    if (tempView.isEmpty) {
      val name = globalViewName.get(plan)
      if (name.isDefined) getInfo(name.get)
      else None
    } else getInfo(tempView.head._1)
  }

  //TODO: 在增加了ExternalCatalog之后进行
  def persistMetaData: Unit = {}

  override def createGlobalTempView(
                                     name: String,
                                     viewDefinition: LogicalPlan,
                                     overrideIfExists: Boolean): Unit = {
    super.createGlobalTempView(name, viewDefinition, overrideIfExists)

    val viewName = formatTableName(name)

    globalViewName.put(getGlobalTempView(viewName).get, viewName)
    val df = Dataset(sqlContext.sparkSession, viewDefinition).toDF()
    createTempViewInfo(viewName, df, 1.0, overrideIfExists, isGlobal = true)
  }

  override def alterTempViewDefinition(name: TableIdentifier, viewDefinition: LogicalPlan): Boolean = {
    val hasView = super.alterTempViewDefinition(name, viewDefinition)

    if (hasView) {
      val viewName = formatTableName(name.table)
      if (name.database.isEmpty) {
        if (tempTableInfo.contains(viewName)) {
          val df = Dataset(sqlContext.sparkSession, viewDefinition).toDF()
          createTempViewInfo(viewName, df, 1.0, true, false)
          true
        } else false
      } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
        val df = Dataset(sqlContext.sparkSession, viewDefinition).toDF()
        createTempViewInfo(viewName, df, 1.0, true, true)
        true
      } else {
        false
      }
    } else false
  }

  def createTempViewInfo(table: String,
                         info: TableInfo,
                         overrideIfExists: Boolean,
                         isGlobal: Boolean): Unit = synchronized {
    if (isGlobal) {
      if (globalTableInfo.contains(table) && !overrideIfExists) {
        throw new TempTableAlreadyExistsException(table)
      }
      globalTableInfo.put(table, info)
    } else {
      if (tempTableInfo.contains(table) && !overrideIfExists) {
        throw new TempTableAlreadyExistsException(table)
      }
      tempTableInfo.put(table, info)
    }
  }

  def createTempViewInfo(name: String,
                         size: Long,
                         count: Long,
                         cardinality: Map[String, Long],
                         sample: DataFrame,
                         p: Double,
                         overrideIfExists: Boolean,
                         isGlobal: Boolean): Unit ={
    val viewName = formatTableName(name)
    createTempViewInfo(viewName,
      TableInfo(viewName, size, count, cardinality, sample, p),
      overrideIfExists, isGlobal)
  }

  def createTempViewInfo(
                          name: String,
                          dataFrame: DataFrame,
                          fraction: Double,
                          overrideIfExists: Boolean,
                          isGlobal: Boolean): Unit = {
    val statistics = new Statistics(dataFrame, sqlContext, fraction)
    createTempViewInfo(name, statistics.getSize, statistics.getCount,
      statistics.getCardinality, statistics.getSample, fraction,
      overrideIfExists, isGlobal)
  }
}
