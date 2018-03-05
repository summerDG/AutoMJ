package org.pasalab.automj

import org.apache.spark.{MjStatistics, SampleStat}
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin, Statistics}
import org.apache.spark.sql.execution.KeysAndTableId
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-7.
 */
//TODO: catalog可能在属性重排序的时候用到, 现在的策略不需要
case class ShareStrategy(override val catalog: MjSessionCatalog, conf: SQLConf)  extends OneRoundStrategy(catalog, conf) {
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

    rIdToShare.map {
      case (rId, replicate) => sizesInBytes(rId) * replicate
    }.sum
  }

  override def attrOptimization(closureLength: Int,
                                relations: Seq[LogicalPlan],
                                statistics: Seq[MjStatistics],
                                exprToCid: Map[Long, Int]): Array[Seq[KeysAndTableId]] = {
    val orderedNodes = statistics.zipWithIndex.flatMap {
      case (child, id) =>
        val keys = relations(id).output.filter {
          case a: AttributeReference =>
            exprToCid.contains(a.exprId.id)
        }
        keys.map {
          case e: AttributeReference =>
            val cId = exprToCid(e.exprId.id)
            assert(child.attributeStats.nonEmpty, s"attributeStates is empty!!!!(size: ${child.sizeInBytes}, count: ${child.rowCount})")
            logInfo(s"key: $e, attributes: ${child.attributeStats.keySet.mkString("[",",","]")}")
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

  def generarteValidTree(v: Int, scaned: Set[Int], equivalenceClasses: Seq[Seq[Node[AttributeVertex]]]): MultipleTreeNode = {
    val newScaned = scaned + v
    if (newScaned.size < equivalenceClasses.size) {
      // 找到涉及到的表
      val rIds = equivalenceClasses(v).map {
        case node =>
          node.v.rId
      }
      // 找到表关联的其他等价类
      val eqIds = equivalenceClasses.zipWithIndex.filter {
        case (s, id) =>
          !newScaned.contains(id) && s.filter(n => rIds.contains(n.v.rId)).nonEmpty
      }.map (_._2)
      // eqIds可能为空
      val children = eqIds.map (i => generarteValidTree(i, newScaned, equivalenceClasses))
      MultipleTreeNode(v, children.toArray)
    } else {
      MultipleTreeNode(v, null)
    }
  }
  def generateValidPath(treeNode: MultipleTreeNode, maxLen: Int, pre: Seq[Int], paths: mutable.ArrayBuffer[Seq[Int]]): Unit = {
    val p = pre.:+(treeNode.v)
    if (p.size == maxLen) {
      paths += p
    } else {
      if (treeNode.children != null && treeNode.children.nonEmpty) {
        for (child <- treeNode.children) {
          generateValidPath(child, maxLen, p, paths)
        }
      }
    }
  }
  override def attrOptimization(samples: Seq[SampleStat[Any]], equivalenceClasses: Seq[Seq[Node[AttributeVertex]]]): Array[Seq[KeysAndTableId]] = {
    val trees = (0 to equivalenceClasses.length - 1).map (x => generarteValidTree(x, Set[Int](), equivalenceClasses))
    val paths = mutable.ArrayBuffer[Seq[Int]]()
    for (tree <- trees) {
      generateValidPath(tree, equivalenceClasses.length, Seq[Int](), paths)
    }
    //TODO: 暂时选第一条合理的路径作为新排序, 因为随机路径会导致性能不稳定
    val id = 0
    assert(paths.length > 0, "no valid order!!!!")
    paths(id).map {
      case i =>
        equivalenceClasses(i).map {
          case n =>
            val v = n.v
            KeysAndTableId(v.k, v.rId)
        }
    }.toArray
  }
}
