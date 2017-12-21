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
                       sqlContext: SQLContext,
                       reader: DataFrameReader,
                       location: Option[String] = None)
  extends SessionCatalog (externalCatalog,
    globalTempViewManager, functionRegistry, conf, hadoopConf, parser, functionResourceLoader){

  override def createTempView(name: String,
                              tableDefinition: LogicalPlan,
                              overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempTables.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempTables.put(table, tableDefinition)
  }

  private val globalTableInfo: mutable.HashMap[String, TableInfo] = {
    if (location.isDefined) {
      val fileSystem: FileSystem = FileSystem.get(hadoopConf)
      val tableDirs = fileSystem.listFiles(new Path(location.get), false)
      val map: mutable.HashMap[String, TableInfo] = mutable.HashMap[String, TableInfo]()
      while (tableDirs.hasNext) {
        val tableDir = tableDirs.next()
        val subFiles = fileSystem.listFiles(tableDir.getPath, false)
        val tableName: String = tableDir.getPath.getName
        var size: Long = 0
        var count: Long = 0
        val cardinality: mutable.Map[String, Long] = mutable.Map[String, Long]()
        var sample: DataFrame = null
        var p: Double = 1.0
        while (subFiles.hasNext) {
          val file = subFiles.next()
          file.getPath.getName match {
            case f if f.startsWith("size") =>
              size = f.substring(5).toLong
            case f if f.startsWith("count") =>
              count = f.substring(6).toLong
            case f if f.startsWith("sample") =>
              sample = reader.json(file.getPath.toString)
              p = f.substring(7).toDouble
            case f =>
              val t = f.split("-")
              val field = t(0)
              val card = t(1).toLong
              cardinality += field -> card
          }
        }
        map += tableName -> TableInfo(tableName, size, count, cardinality.toMap, sample, p)
      }
      map
    } else mutable.HashMap[String, TableInfo]()
  }

  val tempTableInfo: mutable.HashMap[String, TableInfo] = new mutable.HashMap[String, TableInfo]()

  val addedTables: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()

  def getInfo(tableName: String): Option[TableInfo] = globalTableInfo.get(tableName)
  def registerTable(tableName: String, info: TableInfo): Unit = {
    globalTableInfo += tableName -> info
    addedTables += tableName
  }

  def registerTable(tableName: String,
                    size: Long,
                    count: Long,
                    cardinality: Map[String, Long],
                    sample: DataFrame, p: Double): Unit = {
    val name = formatTableName(tableName)
    registerTable(name, TableInfo(name, size, count, cardinality, sample, p))
  }

  def registerTable(tableName: String, dataFrame: DataFrame, fraction: Double): Unit ={

    val statistics = new Statistics(dataFrame, sqlContext, fraction)
    registerTable(tableName, statistics.getSize, statistics.getCount, statistics.getCardinality, statistics.getSample, fraction)
  }

  def getInfo(plan: LogicalPlan): Option[TableInfo] = {
    plan match {
      case SubqueryAlias(table, viewDef) =>
        if (tempTableInfo.contains(table)) tempTableInfo.get(table)
        else globalTableInfo.get(table)
    }
  }

  def persistMetaData: Unit ={
    if (location.isDefined) {
      val l = location.get
      val fileSystem: FileSystem = FileSystem.get(hadoopConf)
      for (tableName <- addedTables; if globalTableInfo.contains(tableName)) {
        val info = globalTableInfo.get(tableName).get
        //生成size文件
        val size = fileSystem.create(new Path(l+"/size-"+info.size))
        size.close()
        //生成count文件
        val count = fileSystem.create(new Path(l+"/count-"+info.count))
        count.close()
        //为每个field生成对应的cardinality文件
        for ((f, c)<- info.cardinality) {
          val field = fileSystem.create(new Path(l+"/"+f+"-"+c))
          field.close()
        }
        info.sample.write.json(l + "/sample")
      }
      globalTableInfo.clear()
    }
    addedTables.clear()
  }

  override def createGlobalTempView(
                                     name: String,
                                     viewDefinition: LogicalPlan,
                                     overrideIfExists: Boolean): Unit = {
    super.createGlobalTempView(name, viewDefinition, overrideIfExists)

    val viewName = formatTableName(name)
    val df = Dataset(sqlContext.sparkSession, viewDefinition).toDF()
    registerTable(viewName, df, 1.0)
  }

  override def alterTempViewDefinition(name: TableIdentifier, viewDefinition: LogicalPlan): Boolean = {
    val r = super.alterTempViewDefinition(name, viewDefinition)

    val viewName = formatTableName(name.table)
    if (tempTableInfo.contains(viewName)) {

    }
  }

  def createTempViewInfo(name: String, info: TableInfo, overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempTableInfo.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempTableInfo.put(table, info)
  }
}
