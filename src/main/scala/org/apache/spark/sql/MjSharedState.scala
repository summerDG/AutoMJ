package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.automj.MjExternalCatalog
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.internal.SharedState

/**
 * Created by wuxiaoqi on 17-12-21.
 */
class MjSharedState(override val sparkContext: SparkContext) extends SharedState(sparkContext){
  override lazy val externalCatalog: ExternalCatalog = {
    val externalCatalog = new MjExternalCatalog(sparkContext.conf, sparkContext.hadoopConfiguration)

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
