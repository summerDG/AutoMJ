package org.apache.spark.sql.automj

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog

/**
 * Created by wuxiaoqi on 17-12-20.
 */
//TODO: ExternalCatalog是外部的数据库系统，所以这里不仅要存储统计数据，还需要存储表的元数据
class MjExternalCatalog(location: Option[String],
                        reader: DataFrameReader,
                         conf: SparkConf = new SparkConf,
                         hadoopConfig: Configuration = new Configuration)
  extends InMemoryCatalog(conf, hadoopConfig) {
//  private val externalTableInfo: mutable.HashMap[String, TableInfo] = {
//    if (location.isDefined) {
//      val fileSystem: FileSystem = FileSystem.get(hadoopConfig)
//      val tableDirs = fileSystem.listFiles(new Path(location.get), false)
//      val map: mutable.HashMap[String, TableInfo] = mutable.HashMap[String, TableInfo]()
//      while (tableDirs.hasNext) {
//        val tableDir = tableDirs.next()
//        val subFiles = fileSystem.listFiles(tableDir.getPath, false)
//        val tableName: String = tableDir.getPath.getName
//        var size: Long = 0
//        var count: Long = 0
//        val cardinality: mutable.Map[String, Long] = mutable.Map[String, Long]()
//        var sample: DataFrame = null
//        var p: Double = 1.0
//        while (subFiles.hasNext) {
//          val file = subFiles.next()
//          file.getPath.getName match {
//            case f if f.startsWith("size") =>
//              size = f.substring(5).toLong
//            case f if f.startsWith("count") =>
//              count = f.substring(6).toLong
//            case f if f.startsWith("sample") =>
//              sample = reader.json(file.getPath.toString)
//              p = f.substring(7).toDouble
//            case f =>
//              val t = f.split("-")
//              val field = t(0)
//              val card = t(1).toLong
//              cardinality += field -> card
//          }
//        }
//        map += tableName -> TableInfo(tableName, size, count, cardinality.toMap, sample, p)
//      }
//      map
//    } else mutable.HashMap[String, TableInfo]()
//  }
//  def getInfo(table: String): Option[TableInfo] = {
//    externalTableInfo.get(table)
//  }
}
