package org.apache.spark.sql.execution.exchange

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

/**
 * Created by wuxiaoqi on 17-12-4.
 */
case class ShareExchange(puppetPartitioning: Partitioning,
                         var projExprs: Seq[Expression],
                         child: SparkPlan,
                         id: Int) extends Exchange {
  override protected def doExecute(): RDD[InternalRow] = ???
}
