package org.pasalab.automj

import org.apache.spark.SampleStat
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-8.
 */
case class EstimatorBasedSample(catalog: MjSessionCatalog, conf: SQLConf) extends JoinSizeEstimator(catalog, conf) {
  override protected def costCore: BigInt = {
//    //TODO: 调整join condition, 并且搞懂joinery.DataFrame的用法之后再具体修改
//    // 用于标记已经用过的条件, 这个作用对于环形的查询来说是有必要的
//    val marked: mutable.Set[(Int, Int)] = mutable.HashSet[(Int, Int)]()
//    // 用于标记已经再语法树中的表
//    val scanned: mutable.Set[Int] = mutable.HashSet[Int]()
//    val edges: Map[Int, Seq[Int]] = joinConditions.toSeq.map(_._1).flatMap {
//      case (l, r) =>
//        Seq[(Int, Int)]((l, r), (r, l))
//    }.groupBy(_._1).map {
//      case (k, v) =>
//        (k, v.map(_._2))
//    }
//
//    val firstId = {
//      val endNode = edges.filter(p => p._2.length == 1)
//      if (endNode.isEmpty) 0
//      else endNode.map(_._1).min
//    }
//    var currentVertices:Array[Int] = Array[Int](firstId)
//
//    var probability: Double = 1.0
//    var join: DataFrame[Any] = null
//    var communication: BigInt = 0
//
//    def generateJoin(r: Int): Unit = {
//      val plan = relations(r)
//      val sampleStat: Option[SampleStat[Any]] = catalog.getSample(plan)
//      probability *= sampleStat.get.fraction
//      assert(sampleStat.isDefined, s"some relation(${sampleStat.get}) has no sample")
//      if (join == null) {
//        join = sampleStat.get.sample
//      } else {
//        // joinery.DataFrame的join操作执行过后会修改列名, 左侧会将join列名增加_left, 右侧的join key会增加_right
//        val conditions = scanned.flatMap {
//          case l if (joinConditions.contains(l, r) || joinConditions.contains(r, l))=>
//            if ((l < r && !marked.contains((l, r)) || (l >= r && !marked.contains(r, l)))) {
//              if (l < r) marked.add((l, r))
//              else marked.add(r, l)
//              val (lks, rks) = if (joinConditions.contains((l, r))) joinConditions((l, r)) else joinConditions((r, l))
//              lks.zip(rks).map {
//                case (lk, rk) =>
//                  val oldName = {
//                    val rawName = lk.asInstanceOf[NamedExpression].name
//                    if (join.columns().contains(rawName)) rawName
//                    else if (join.columns().contains(rawName + "_left")) rawName + "_left"
//                    else rawName + "_right"
//                  }
//                  (oldName, rk.asInstanceOf[NamedExpression].name)
//              }
//            }
//            else None
//          case _ => None
//        }
//        for ((oldName, newName) <- conditions) {
//          join.rename(oldName, newName)
//        }
//
//        try {
//          join = join.join(sampleStat.get.sample)
//        } catch {
//          case ex: IllegalArgumentException =>
//            assert(false, join.columns().toArray.mkString(",") + s"  ${ex.getMessage}")
//        }
//        if (scanned.size < relations.size - 1) {
//          communication += (join.size().toLong / probability).toLong
//        }
//        // 将过去的列名修改回来, 否则等到语法树深度加深之后会将列名搞混
//        for ((oldName, newName) <- conditions) {
//          join.rename(newName + "_left", if (oldName.contains("_")) oldName.substring(0,oldName. indexOf("_")) else oldName)
//        }
//      }
//    }
//
//    while (!currentVertices.isEmpty) {
//      val newVertices: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer[Int]()
//      for (v <- currentVertices) {
//        generateJoin(v)
//        scanned += v
//        val neighbourhood = edges(v).filter{
//          case x =>
//            !scanned.contains(x)
//        }
//        newVertices ++= neighbourhood
//      }
//
//      currentVertices = newVertices.toArray
//    }
//
//    communication
    BigInt(conf.getConfString(MjConfigConst.JOIN_DEFAULT_SIZE))
  }

  //TODO: 以后实现基于采用的Join Size预测算法
  override def joinSample(condition: Map[(Int, Int), Option[Expression]], samples: Seq[SampleStat[Any]]): SampleStat[Any] = {
    samples.head
  }
}
