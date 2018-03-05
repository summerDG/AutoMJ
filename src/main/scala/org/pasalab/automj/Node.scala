package org.pasalab.automj

import scala.collection.mutable.ArrayBuffer

/**
  * Created by summerDG on 2018/2/26.
  */
case class Node[T](v: T) {
  var degree: Int = 0
  val nbrs: ArrayBuffer[Node[T]] = new ArrayBuffer[Node[T]]()

  def getDegree: Int = degree
  def setDegree(d: Int): Unit ={
    degree = d
  }
  def getNbrs: Seq[Node[T]] = nbrs
  def setNbrs(nbs: Seq[Node[T]]): Unit ={
    nbrs.clear()
    for (n <- nbs) {
      nbrs += n
    }
  }
  def hasNbr(n: Node[T]) : Boolean = {
    nbrs.contains(n)
  }
  def addNode(n: Node[T]): Unit = {
    this.nbrs += n
    this.degree += 1
  }
  def removeNbr(n: Node[T]): Unit = {
    this.nbrs -= n
    this.degree -= 1
  }
}
