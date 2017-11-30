package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wuxiaoqi on 17-11-30.
 */
class HyperGraph(V: Seq[HyperGraphVertex], E: Seq[HyperGraphEdge]) {

}
object HyperGraph {
  def apply(plan: LogicalPlan): HyperGraph = {
    plan match {
      case MjExtractor(keysEachRelation,
      originBothKeysEachCondition, otherConditions, relations) =>
        // 找出所有的等价属性
        val bothKeysEachCondition = originBothKeysEachCondition.toMap
        val equivalenceClasses: Seq[Seq[Node]] = connectComponent(bothKeysEachCondition)
        // 每个等价属性相当于HyperGraph中的一个点
        val vBuf: ArrayBuffer[HyperGraphVertex] = ArrayBuffer[HyperGraphVertex]()
        // 每个relation相当于HyperGraph中的边, 包含的属性是HyperGraph中的点
        val eBuf: Array[(LogicalPlan, ArrayBuffer[HyperGraphVertex])] =
          new Array[(LogicalPlan, ArrayBuffer[HyperGraphVertex])](relations.length)

        for (i <- 0 to equivalenceClasses.length - 1) {
          vBuf(i) = HyperGraphVertex(i, equivalenceClasses(i).map(_.k))
          for (node <- equivalenceClasses(i)) {
            val rId = node.rId
            if (eBuf(rId) == null) {
              eBuf(rId) = (relations(rId), ArrayBuffer[HyperGraphVertex]())
            }
            eBuf(rId)._2 += vBuf(i)
          }
        }
        val vertices: Seq[HyperGraphVertex] = vBuf
        val edges: Seq[HyperGraphEdge] = eBuf.map {
          case (p: LogicalPlan, v: ArrayBuffer[HyperGraphVertex]) => HyperGraphEdge(p, v)
        }.toSeq
        new HyperGraph(vertices, edges)
    }
  }

  /**
   * 通过join关系构造图
   * @param m (left relation id, right relation id) -> join condition
   * @return 图
   */
  def createNodesAndEdges(m: Map[(Int, Int), (Seq[Expression], Seq[Expression])]): (Seq[Node], Seq[Edge]) = {
    m.map {
      case ((l, r), (lk, rk)) =>
        val lNodes = lk.map(new Node(l, _))
        val rNodes = rk.map(new Node(r, _))
        val edges = lNodes.zip(rNodes).map {
          case (l, r) => Edge(l, r)
        }

        (lNodes ++ rNodes, edges)
    }.fold((Seq(), Seq()))((x, y) => ((x._1 ++ y._1), x._2 ++ y._2))
  }

  /**
   * 找图中的强联通分量
   * @param bothKeysEachCondition (left relation id, right relation id) -> join condition
   * @return 强联通分量
   */
  def connectComponent(bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])]): Seq[Seq[Node]] = {
    val (nodes, edges) = createNodesAndEdges(bothKeysEachCondition)
    val nodeToId = nodes.toSet[Node].zipWithIndex.toMap
    val v = nodeToId.unzip._2.toSeq
    val e = edges.map(e => (nodeToId(e.n1), nodeToId(e.n2))).toSet.toSeq

    val G = new Graph(v, e)

    val marked = new Array[Boolean](G.V)
    val id = new Array[Int](G.V)
    var count = 0

    def dfs(v: Int): Unit = {
      marked(v) = true
      id(v) = count
      for (w <- G.adj(v)) {
        if (!marked(w)) dfs(w)
      }
    }

    var s = 0
    while (s < G.V) {
      if (!marked(s)) {
        dfs(s)
        count += 1
      }
      s += 1
    }

    val idToNode = nodeToId.map(x => (x._2, x._1))
    val closures = new Array[ArrayBuffer[Node]](count)

    var i = 0
    while (i < G.V) {
      val node = idToNode(i)
      if (closures(id(i)) == null) {
        closures(id(i)) = ArrayBuffer(node)
      } else {
        closures(id(i)) += node
      }
      i += 1
    }
    assert(closures.nonEmpty && closures.forall(_.nonEmpty), "connectComponent is empty")
    closures
  }
}
case class Node(rId: Int, k: Expression)
case class Edge(n1: Node, n2: Node)
class Graph(v: Seq[Int], e: Seq[(Int, Int)]) {
  def V: Int = v.length
  def E: Int = e.length
  def adj(n: Int): Seq[Int] = {
    e.flatMap{
      case (n1, n2) =>
        if (n1 == n) Some(n2)
        else if (n2 == n) Some(n1)
        else None
    }
  }
}