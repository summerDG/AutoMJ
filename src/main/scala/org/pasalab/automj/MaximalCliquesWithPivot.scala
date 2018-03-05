package org.pasalab.automj

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by summerDG on 2018/2/26.
  */
class MaximalCliquesWithPivot(nodesCount: Int, edges: Seq[(Int, Int)]) {
  val maxCliques: ArrayBuffer[Array[Node[Int]]] = new ArrayBuffer[Array[Node[Int]]]()
  val graph: ArrayBuffer[Node[Int]] = {
    val g = new ArrayBuffer[Node[Int]]()
    for (i <- 0 to nodesCount - 1) {
      g += Node[Int](i)
    }
    edges.foreach {
      case (l, r) =>
        g(l).addNode(g(r))
        g(r).addNode(g(l))
    }
    g
  }

  def maxCliqueSize: Int = {
    if (maxCliques == null) 0
    else maxCliques.size
  }

  def getMaxCliques: Array[Array[Node[Int]]] = maxCliques.toArray

  def Bron_KerboschWithPivot(R: Set[Node[Int]], P: Set[Node[Int]], X: Set[Node[Int]]): Unit ={
    if (P.size == 0 && X.size == 0) {
      // output R
      if(R.size >= 2) maxCliques += R.toArray
    } else {
      val P1: collection.mutable.Set[Node[Int]] = new mutable.HashSet[Node[Int]]()
      val X1: collection.mutable.Set[Node[Int]] = new mutable.HashSet[Node[Int]]()
      for (n <- P) {
        P1 += n
      }
      for (n <- X) {
        X1 += n
      }
      // find pivot
      val u = (P union X).maxBy(_.degree)

      // for each vertex v in P \ N(u)
      for (v <- (P -- u.nbrs)) {
        Bron_KerboschWithPivot(R union Set[Node[Int]](v),
        P1.toSet intersect v.getNbrs.toSet, X1.toSet intersect v.getNbrs.toSet)
        P1 -= v
        X1 += v
      }
    }
  }
  def Bron_KerboschPivotExecute(): Unit = {
    val R = Set[Node[Int]]()
    val P = this.graph.toSet
    val X = Set[Node[Int]]()
    Bron_KerboschWithPivot(R, P, X)
  }
}
