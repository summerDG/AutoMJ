package org.pasalab.automj

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.automj.MjSessionCatalog

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-8.
 */
case class EstimatorBasedSample(catalog: MjSessionCatalog, conf: SparkConf) extends JoinSizeEstimator(catalog, conf) {
  override protected def costCore: Long = {
    val samples:Seq[DataFrame] = getSamples()

    val probability: Seq[Double] = getProbability()

    val marked: mutable.Set[Int] = mutable.Set[Int]()

    val markedCondition: mutable.Set[(Int, Int)] = mutable.Set[(Int, Int)]()

    val edges: Map[Int, Seq[Int]] = joinConditions.toSeq.map(_._1).flatMap {
      case (l, r) =>
        Seq[(Int, Int)]((l, r), (r, l))
    }.groupBy(_._1).map {
      case (k, v) =>
        (k, v.map(_._2))
    }
    var currentVertices:Array[Int] = Array[Int](0)

    var join = samples(0)

    def generateJoin(v: Int, n: Int): Unit = {
      val condition = joinConditions((v, n))
      join = join.join(samples(n), condition)
    }

    var communication: Long = 0
    while (!currentVertices.isEmpty) {
      val newVertices: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer[Int]()

      for (v <- currentVertices) {
        marked.add(v)
        val neighbourhood = edges(v).filter(x => !marked.contains(x))

        for (n <- neighbourhood) {
          val others = edges(n)
            .filter(x => marked.contains(x) && !(markedCondition.contains((x, n)) || markedCondition.contains(n, x)))
          p *= probability(n)
          for (o <- others) {
            assert(joinConditions.contains((o, n))|| joinConditions.contains((n, o)), "construct graph problem")
            if (joinConditions.contains((o, n))) {
              generateJoin(o, n)
              markedCondition.add((o, n))
            } else {
              generateJoin(n, o)
              markedCondition.add((o, n))
            }
          }
          //TODO: persist后性能会好
          communication += join.count() / p
        }
        newVertices ++= neighbourhood
      }

      currentVertices = newVertices.toArray
    }
    communication
  }
}
