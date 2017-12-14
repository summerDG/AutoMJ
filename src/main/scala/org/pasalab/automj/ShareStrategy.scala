package org.pasalab.automj
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin}
import org.apache.spark.sql.execution.KeysAndTableId

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-7.
 */
case class ShareStrategy(catalog: MjSessionCatalog)  extends OneRoundStrategy(catalog) {
  override protected def optimizeCore: LogicalPlan = {
    ShareJoin(reorderedKeysEachTable, relations, bothKeysEachCondition, otherCondition,
      numShufflePartitions, shares, dimensionToExprs, closures)
  }

  override protected def costCore: Long = {
    val rIdToShare = closures.map(s => s.map(_._2)).zipWithIndex.flatMap {
      case (rIds, sId) => rIds.map(r => (r, sId))
    }.groupBy(_._1).map {
      case (rId, sIds) => (rId, numShufflePartitions / sIds.map(x => shares(x._2)).fold(1)(_ * _))
    }
    val statistics = relations.map(x => catalog.getInfo(x))

    rIdToShare.map {
      case (rId, replicate) => statistics(rId).size * replicate
    }.sum
  }

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
    val newOrderedAttr = new Array[mutable.ArrayBuffer[KeysAndTableId]](closureLength)
      .map(_ => mutable.ArrayBuffer[KeysAndTableId]())
    val ascend: mutable.Map[Int, Int] = mutable.Map[Int, Int]()
    var currentId = 0
    for ((exprs, cId, _, id) <- orderedNodes) {
      if (ascend.contains(cId)) {
        newOrderedAttr(ascend(cId)) += KeysAndTableId(exprs :: Nil, id)
      } else {
        ascend += cId -> currentId
        currentId += 1
        newOrderedAttr(currentId) += KeysAndTableId(exprs :: Nil, id)
      }
    }

    newOrderedAttr.map(_.sortBy(_.tableId).toSeq)
  }
}
