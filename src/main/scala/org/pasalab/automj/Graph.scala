package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wuxiaoqi on 17-12-1.
 */
class Graph[T](v: Seq[Int], e: Seq[(Int, Int)], nodeToId: Map[Node[T], Int]) {
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
  /**
   * 找图中的强联通分量
   * @return 强联通分量
   */
  def connectComponent(): Seq[Seq[Node[T]]] = {
    val G = this

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
    val closures = new Array[ArrayBuffer[Node[T]]](count)

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
object Graph {
  /**
   * 通过join关系构造图
   * @param m (left relation id, right relation id) -> join condition
   * @return 图
   */
  def apply[T](m: Seq[(T, T)]): Graph[T] = {
    val (nodePair, edges) = m.map {
      case (l, r) =>
        val lNode = Node(l)
        val rNode = Node(r)
        val edge = Edge(lNode, rNode)

        ((lNode, rNode), edge)
    }.unzip
    val nodes = nodePair.flatMap(x => Seq[Node[T]](x._1, x._2))
    val nodeToId = nodes.toSet[Node[T]].zipWithIndex.toMap
    val v = nodeToId.unzip._2.toSeq
    val e = edges.map(e => (nodeToId(e.n1), nodeToId(e.n2))).toSet.toSeq

    new Graph(v, e, nodeToId)
  }
}
case class Node[T](v: T)
case class Edge[T](n1: Node[T], n2: Node[T])
