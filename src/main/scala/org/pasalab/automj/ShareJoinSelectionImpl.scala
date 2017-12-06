package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{KeysAndTableId, ShareJoinSelection}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wuxiaoqi on 17-12-5.
 * 顺序优化算法实现：就是按照cardinality大小进行排序
 */
case class ShareJoinSelectionImpl(meta: MetaManager, conf: SQLConf) extends ShareJoinSelection(meta, conf){
  override def attrOptimization(closureLength: Int,
                                relations: Seq[LogicalPlan],
                                statistics: Seq[TableInfo],
                                exprToCid: Map[ExprId, Int]): Array[Seq[KeysAndTableId]] = {
    val orderedNodes = statistics.zipWithIndex.flatMap {
      case (child, id) =>
        val keys = relations(id).output
        keys.map {
          case e: AttributeReference =>
            val cId = exprToCid(e.exprId)
            val card = child.getCardinality(e)
            (e, cId, card, id)
        }
    }.sortBy(_._3)
    val newOrderedAttr = new Array[ArrayBuffer[KeysAndTableId]](closureLength)
      .map(_ => ArrayBuffer[KeysAndTableId]())
    for ((exprs, cId, _, id) <- orderedNodes) {
      newOrderedAttr(cId) += KeysAndTableId(exprs :: Nil, id)
    }
    logInfo(s"[POS]before new order attributes, orderedNodes length: ${orderedNodes.length}," +
      s"newOrderedAttr ${newOrderedAttr.map(_.length.toString).reduceLeft(_ + "," + _)}")
    //    for (cId <- 0 until closureLength) {
    //      logInfo(s"closure Id: $cId, ${newOrderedAttr(cId).fold("")(_.toString + _)}")
    //    }
    val x = newOrderedAttr.map(_.toSeq)
    logInfo("[POS]after new order attributes")
    x
  }
}
