package org.apache.spark.sql.automj

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Statistics, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, InMemoryCatalog}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.pasalab.automj.TableInfo

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-20.
 */
class MjExternalCatalog(
                         conf: SparkConf = new SparkConf,
                         hadoopConfig: Configuration = new Configuration) extends InMemoryCatalog(conf, hadoopConfig) {
}
