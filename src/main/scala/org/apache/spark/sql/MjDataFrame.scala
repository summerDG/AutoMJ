package org.apache.spark.sql

import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Created by wuxiaoqi on 17-12-21.
 */
class MjDataFrame[T](dataframe: Dataset[T],
                     override val sparkSession: SparkSession,
                     override val logicalPlan: LogicalPlan,
                     encoder: Encoder[T])
  extends Dataset[T](sparkSession, logicalPlan, encoder){
  def this(df: Dataset[T]) = {
    this(df, df.sparkSession, df.logicalPlan, df.exprEnc)
  }
//  def createStatistics(name: String, overrideIfExists: Boolean): Unit ={
//    val catalog: MjSessionCatalog = sparkSession.sessionState.catalog.asInstanceOf[MjSessionCatalog]
//    catalog.createStatistics(name, dataframe.toDF(), overrideIfExists)
//  }
//  def createStatistics(name: String): Unit = {
//    createStatistics(name, true)
//  }
}

object MjDataFrame {
  def apply[T](df: Dataset[T]): MjDataFrame[T] = new MjDataFrame(df)
}
