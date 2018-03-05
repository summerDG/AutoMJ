package org.pasalab.automj

import org.apache.spark.SampleStat
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.pasalab.automj.MjExtractor.ConditionMatrix

import scala.collection.mutable

/**
  * Created by summerDG on 2018/2/27.
  */
case class MultipleTreeNode(v: Int, children: Array[MultipleTreeNode]) {
  def treeToLogicalPlanWithSample(useSample: Boolean,
                                  relations: Seq[LogicalPlan],
                                  keysEachCondition: ConditionMatrix,
                                  catalog: MjSessionCatalog,
                                  oneRoundStrategy: OneRoundStrategy,
                                  multiRoundStrategy: MultiRoundStrategy,
                                  joinSizeEstimator: JoinSizeEstimator,
                                  partitionNum: Int = 100,
                                  defaultMultiRoundCost: Long = 100,
                                  scale: Int = 10000): (LogicalPlan, Set[Int], Option[SampleStat[Any]], Long) = {
    if (this.children == null || this.children.isEmpty) {
      val plan = relations(this.v)
      val statistics = catalog.getStatistics(plan)
      val sample: Option[SampleStat[Any]] = if (useSample) {
        statistics.map(_.extractSample)
      } else None
      val size = statistics.map(_.rowCount.get.toLong / scale).reduce(_ * _) * scale
      (plan, Set[Int](this.v), sample, size)
    } else {
      if (this.children.size >= 3) {
        // 判断哪种Join方式的通信量较少
        val planWithSamples = this.children
          .map (_.treeToLogicalPlanWithSample(true, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, defaultMultiRoundCost, scale))
        val idsEachChild = planWithSamples.map(_._2)
        val samples = planWithSamples.map(_._3).map(_.get)
        val plans = planWithSamples.map(_._1)
        val sizes = planWithSamples.map(_._4)
        val idMap: Map[Int, Int] = idsEachChild.zipWithIndex.flatMap {
          case (ids, newId) =>
            ids.map(x => (x, newId))
        }.toMap
        val joinConditions = {
          val filteredMap = keysEachCondition.filter {
            case ((l, r), k) =>
              idMap.contains(l) && idMap.contains(r) && idMap(l) != idMap(r)
          }
          val m: mutable.Map[(Int, Int), (Seq[Expression], Seq[Expression])] =
            new mutable.HashMap[(Int, Int), (Seq[Expression], Seq[Expression])]()
          for (((l, r), (lk, rk)) <- filteredMap) {
            val k = (idMap(l), idMap(r))
            if (m.contains(k)) {
              val (oldLK, oldRK) = m(k)
              m.put(k,(oldLK ++ lk, oldRK ++ rk))
            } else {
              m.put(k, (lk, rk))
            }
          }
          m.toMap
        }
//        val sizes: Seq[Long] = samples.map {
//          case s =>
//            (s.sample.size / s.fraction).toLong
//        }
//        val keys = {
//          val keysEachTable = new Array[Set[Expression]](plans.length).map (_ => Set[Expression]())
//          for (((l, r), (lk, rk)) <- joinConditions) {
//            keysEachTable(l) ++= lk
//            keysEachTable(r) ++= rk
//          }
//          keysEachTable.map (_.toSeq)
//        }
        // 顺便把refresh的工作也做了
        oneRoundStrategy.loadArgument(joinConditions, plans, sizes, samples, partitionNum)
        val oneRoundCost = oneRoundStrategy.cost()
        val condMap = joinConditions.map {
          case (ids, (lk, rk)) =>
            val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
            ids -> Some(cond)
        }
        val multiRoundSample = joinSizeEstimator.joinSample(condMap, samples)
        //        val multiRoundCost = BigInt((multiRoundSample.sample.size / multiRoundSample.fraction).toLong)
        //TODO: 由于现在Jounery.DataFrame[T]不好用, 所以先使用这种方法预测join size
        val size = sizes.reduce(_ * _) * scale
        val multiRoundCost = BigInt(size)
        val idsSet = idsEachChild.fold(Set[Int]())(_ ++ _)
        val sample = if (useSample) Some(multiRoundSample) else None
        //TODO: 由于现在Jounery.DataFrame[T]不好用, 所以先使用这种方法预测join size
        if (oneRoundCost < multiRoundCost) {
          // 生成ShareJoin
          val plan = oneRoundStrategy.optimize()
          (plan, idsSet, sample, size)
        } else {
          // 生成Join
          val plan = multiRoundStrategy.optimize(joinConditions, plans)
          (plan, idsSet, sample, size)
        }
      } else {
        val (left, leftIds, leftSample, leftSize) = children(0).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, defaultMultiRoundCost, scale)
        val (right, rightIds, rightSample, rightSize) = children(1).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, defaultMultiRoundCost, scale)

        val condition = keysEachCondition.flatMap {
          case ((l, r), (lk, rk)) =>
            if (leftIds.contains(l) && rightIds.contains(r)) {
              val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
              Some(cond)
            }
            else None
        }.reduce((l, r) => And(l, r))

        val plan = Join(left, right, Inner, Some(condition))
        val condMap = Map[(Int, Int), Option[Expression]]((0,1) -> Some(condition))
        val sample = if (useSample) {
          Some(joinSizeEstimator.joinSample(condMap, Seq[SampleStat[Any]](leftSample.get, rightSample.get)))
        } else {
          None
        }
        //TODO: 由于现在Jounery.DataFrame[T]不好用, 所以先使用这种方法预测join size
        (plan, leftIds union rightIds, sample, ((leftSize/scale) * (rightSize/scale))*scale)
      }
    }
  }
  override def toString: String = {
    val head = v.toString
    val pre = new StringBuilder()
    pre.append("([")
    pre.append(head)
    pre.append("]")
    if (children == null) {
      pre.append(")")
    } else {
      pre.append("{")
      pre.append(children.map(_.toString).mkString(","))
      pre.append("})")
    }
    pre.toString()
  }
}
