package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wuxiaoqi on 17-11-30.
 */
class HyperGraph(V: Seq[HyperGraphVertex], E: Array[HyperGraphEdge]) {
  def removeEdge(rId: Int, vIds: Seq[Int]): HyperGraph = {
    vIds.foreach(vId => V(vId) --)
    E(rId) = null
    this
  }
}
object HyperGraph {
  def apply(multiRoundCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
            relations: Seq[LogicalPlan], rIds: Seq[Int]): HyperGraph = {
    // 找出所有的等价属性
    val joinEdges = multiRoundCondition.map {
      case ((l, r), (lk, rk)) =>
        (AttributeVertex(l, lk), AttributeVertex(r, rk))
    }.toSeq
    val equivalenceClasses: Seq[Seq[Node[AttributeVertex]]] = Graph(joinEdges).connectComponent(_.rId)
    // 每个等价属性相当于HyperGraph中的一个点
    val vBuf: ArrayBuffer[HyperGraphVertex] = ArrayBuffer[HyperGraphVertex]()
    // 每个relation相当于HyperGraph中的边, 包含的属性是HyperGraph中的点
    val eBuf: Array[ArrayBuffer[Int]] =
    new Array[ArrayBuffer[Int]](relations.length)

    for (i <- 0 to equivalenceClasses.length - 1) {
      vBuf += HyperGraphVertex(i, equivalenceClasses(i).flatMap(_.v.k))
      for (node <- equivalenceClasses(i)) {
        val rId = node.v.rId
        if (eBuf(rId) == null) {
          eBuf(rId) = ArrayBuffer[Int]()
        }
        eBuf(rId) += i
      }
    }

    // 另外还有一部分点是不参与Join的，otherVId对这些点进行编号
    var otherVId = equivalenceClasses.length
    // 每张表也会包含不参与Join的点
    val otherVertices: Array[ArrayBuffer[Int]] =
    new Array[ArrayBuffer[Int]](relations.length)

    // HyperGraph必须排除掉一轮Join的节点
    for (rId <- rIds) {
      val relation = relations(rId)
      for (attr <- relation.output if eBuf(rId).contains(attr)) {
        vBuf += HyperGraphVertex(otherVId, Seq[Expression](attr))
        otherVertices(rId) += otherVId
        otherVId += 1
      }
    }
    val vertices: Seq[HyperGraphVertex] = vBuf
    val edges: Array[HyperGraphEdge] = {
      for (rId <- 0 to relations.length - 1)
        yield HyperGraphEdge(relations(rId), eBuf(rId)++otherVertices(rId))
    }.toArray
    new HyperGraph(vertices, edges)
  }
}
case class AttributeVertex(rId: Int, k: Seq[Expression])