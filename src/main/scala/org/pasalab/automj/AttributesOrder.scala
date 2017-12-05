package org.pasalab.automj

import org.apache.spark.sql.execution.KeysAndTableId

/**
 * Created by wuxiaoqi on 17-12-5.
 */
trait AttributesOrder {
  def attrOptimization(closureLength: Int,
                       children: Seq[TableInfo]): Array[Seq[KeysAndTableId]]
}
