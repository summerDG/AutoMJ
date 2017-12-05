package org.pasalab.automj

import org.apache.spark.sql.execution.{KeysAndTableId, ShareJoinSelection}
import org.apache.spark.sql.internal.SQLConf

/**
 * Created by wuxiaoqi on 17-12-5.
 */
case class ShareJoinSelectionImpl(meta: MetaManager, conf: SQLConf) extends ShareJoinSelection(meta, conf){
  override def attrOptimization(closureLength: Int, children: Seq[TableInfo]): Array[Seq[KeysAndTableId]] = ???
}
