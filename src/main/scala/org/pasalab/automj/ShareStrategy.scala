package org.pasalab.automj

import org.apache.spark.MjStatistics
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin, Statistics}
import org.apache.spark.sql.execution.KeysAndTableId
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-7.
 */
//TODO: catalog可能在属性重排序的时候用到, 现在的策略不需要
case class ShareStrategy(catalog: MjSessionCatalog, conf: SQLConf)  extends OneRoundStrategy(catalog, conf) {
  override protected def optimizeCore: LogicalPlan = {
    ShareJoin(reorderedKeysEachTable, relations, bothKeysEachCondition, otherCondition,
      numShufflePartitions, shares, dimensionToExprs, closures)
  }

  override protected def costCore(): BigInt = {
    val rIdToShare = closures.map(s => s.map(_._2)).zipWithIndex.flatMap {
      case (rIds, sId) => rIds.map(r => (r, sId))
    }.groupBy(_._1).map {
      case (rId, sIds) => (rId, numShufflePartitions / sIds.map(x => shares(x._2)).fold(1)(_ * _))
    }
    val statistics = relations.flatMap(x => catalog.getStatistics(x))
    assert(statistics.length == relations.length, "some relation has no statistics")

    rIdToShare.map {
      case (rId, replicate) => statistics(rId).sizeInBytes * replicate
    }.sum
  }

  override def attrOptimization(closureLength: Int,
                                relations: Seq[LogicalPlan],
                                statistics: Seq[MjStatistics],
                                exprToCid: Map[Long, Int]): Array[Seq[KeysAndTableId]] = {
    val orderedNodes = statistics.zipWithIndex.flatMap {
      case (child, id) =>
        val keys = relations(id).output
        keys.map {
          case e: AttributeReference =>
            val cId = exprToCid(e.exprId.id)
            assert(child.attributeStats.nonEmpty, s"attributeStates is empty!!!!(size: ${child.sizeInBytes}, count: ${child.rowCount})")
            val card = child.attributeStats.get(e).get.distinctCount
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
        newOrderedAttr(currentId) += KeysAndTableId(exprs :: Nil, id)
        currentId += 1
      }
    }

    newOrderedAttr.map(_.sortBy(_.tableId).toSeq)
  }
}
