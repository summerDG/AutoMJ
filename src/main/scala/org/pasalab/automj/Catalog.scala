package org.pasalab.automj

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.mutable

/**
 * 用于从文件系统中加载表的元数据, 元数据以文件名命名, size-{num}, count-{num}, {field}-{cardinality}
 * 样本以json格式存在sample目录下
 * Created by wuxiaoqi on 17-11-28.
 */
class Catalog(location: String, sparkSession: SparkSession) {
  private val tables: mutable.Map[String, TableInfo] = {
    val fileSystem: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val tableDirs = fileSystem.listFiles(new Path(location), false)
    val map: mutable.Map[String, TableInfo] = mutable.Map[String, TableInfo]()
    while (tableDirs.hasNext) {
      val tableDir = tableDirs.next()
      val subFiles = fileSystem.listFiles(tableDir.getPath, false)
      val tableName: String = tableDir.getPath.getName
      var size: Long = 0
      var count: Long = 0
      val cardinality: mutable.Map[String, Long] = mutable.Map[String, Long]()
      var sample: DataFrame = null
      while (subFiles.hasNext) {
        val file = subFiles.next()
        file.getPath.getName match {
          case f if f.startsWith("size") =>
            size = f.substring(5).toLong
          case f if f.startsWith("count") =>
            count = f.substring(6).toLong
          case "sample" =>
            sample = sparkSession.read.json(file.getPath.toString)
          case f =>
            val t = f.split("-")
            val field = t(0)
            val card = t(1).toLong
            cardinality += field -> card
        }
      }
      map += tableName -> TableInfo(size, count, cardinality.toMap, sample)
    }
    map
  }
  val addedTables: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()

  def getTable(tableName: String): Option[TableInfo] = tables.get(tableName)
  def registerTable(tableName: String, info: TableInfo): Unit = {
    tables += tableName -> info
    addedTables += tableName
  }
  def close: Unit ={
    for (tableName <- addedTables; if tables.contains(tableName)) {
      val info = tables.get(tableName).get
      val fileName = tableName + "-" + info.size + "-" + info.count + "-" + info.cardinality
      info.sample.write.json(location + "/" + fileName)
    }
    tables.clear()
    addedTables.clear()
  }
}
