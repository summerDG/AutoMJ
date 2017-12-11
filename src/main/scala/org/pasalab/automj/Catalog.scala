package org.pasalab.automj

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.mutable

/**
 * 用于从文件系统中加载表的元数据, 元数据以文件名命名, size-{num}, count-{num}, {field}-{cardinality}
 * 样本以json格式存在sample-probability目录下
 * Created by wuxiaoqi on 17-11-28.
 */
class Catalog(location: Option[String], sparkSession: SparkSession) {
  private val tables: mutable.Map[String, TableInfo] = {
    if (location.isDefined) {
      val fileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val tableDirs = fileSystem.listFiles(new Path(location.get), false)
      val map: mutable.Map[String, TableInfo] = mutable.Map[String, TableInfo]()
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
              sample = sparkSession.read.json(file.getPath.toString)
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
    } else mutable.Map[String, TableInfo]()
  }
  val addedTables: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()

  def getTable(tableName: String): Option[TableInfo] = tables.get(tableName)
  def registerTable(tableName: String, info: TableInfo): Unit = {
    tables += tableName -> info
    addedTables += tableName
  }
  def close: Unit ={
    if (location.isDefined) {
      val l = location.get
      val fileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      for (tableName <- addedTables; if tables.contains(tableName)) {
        val info = tables.get(tableName).get
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
      tables.clear()
    }
    addedTables.clear()
  }
}
