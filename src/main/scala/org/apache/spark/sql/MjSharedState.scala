package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.automj.MjExternalCatalog
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.SharedState
import org.pasalab.automj.MjConfigConst

/**
 * Created by wuxiaoqi on 17-12-21.
 */
//TODO: 以后在扩展ExternalCatalog的时候再进行更改使用
class MjSharedState(val session: SparkSession) extends SharedState(session.sparkContext){
  override lazy val externalCatalog: MjExternalCatalog = {
    val conf = sparkContext.conf
    val externalCatalog =
      new MjExternalCatalog(
        conf.getOption(MjConfigConst.METADATA_LOCATION),
        session.read,
        conf, sparkContext.hadoopConfiguration)

    val defaultDbDefinition = CatalogDatabase (
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(warehousePath),
      Map()
    )

    if (!externalCatalog.databaseExists(SessionCatalog.DEFAULT_DATABASE)) {
      externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    }

    externalCatalog.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })
    externalCatalog
  }

}
