package org.pasalab.automj

import org.apache.spark.SampleStat
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, TempJoin}
import org.pasalab.automj.MjExtractor.ConditionMatrix

import scala.collection.mutable

/**
  * Created by summerDG on 2018/2/27.
  */
case class MultipleJoinTreeNode(v: Int, children: Array[MultipleJoinTreeNode]) {
  def output(relations: Seq[LogicalPlan]): Seq[Attribute] = {
    if (children == null || children.isEmpty) {
      relations(this.v).output
    } else {
      children.flatMap(_.output(relations))
    }
  }
  def treeToLogicalPlanWithSample(useSample: Boolean,
                                  relations: Seq[LogicalPlan],
                                  keysEachCondition: ConditionMatrix,
                                  catalog: MjSessionCatalog,
                                  oneRoundStrategy: OneRoundStrategy,
                                  multiRoundStrategy: MultiRoundStrategy,
                                  joinSizeEstimator: JoinSizeEstimator,
                                  partitionNum: Int, twoJoinedSize: Long, threeJoinedSize: Long): (LogicalPlan, Set[Int],Option[SampleStat[Any]], Long) = {
    if (this.children == null || this.children.isEmpty) {
      val plan = relations(this.v)
      val statistics = catalog.getStatistics(plan)
      val sample: Option[SampleStat[Any]] = if (useSample) {
        statistics.map(_.extractSample)
      } else None

      //      val size = statistics.map(_.rowCount.get.toLong / scale).reduce(_ * _) * scale
      val count = statistics.get.rowCount.get.longValue()
      (plan, Set[Int](this.v),sample, count)
    } else {
      if (this.children.size >= 3) {

        val planWithSamples = this.children.map {
          case c =>
            c.treeToLogicalPlanWithSample(true, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator,partitionNum, twoJoinedSize, threeJoinedSize)
        }

        val idsEachChild = planWithSamples.map(_._2)
        val samples = planWithSamples.map(_._3).map(_.get)
        val sizes = planWithSamples.map(_._4)
        val plans = planWithSamples.map(_._1)
        val idsSet = idsEachChild.fold(Set[Int]())(_ ++ _)

        // rId -> childId
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

        val condMap = joinConditions.map {
          case (ids, (lk, rk)) =>
            val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
            ids -> Some(cond)
        }
        val multiRoundSample = joinSizeEstimator.joinSample(condMap, samples)
        val sample = if (useSample) Some(multiRoundSample) else None

        oneRoundStrategy.loadArgument(joinConditions, plans, sizes, samples, partitionNum)
        val plan = oneRoundStrategy.optimize()
        (plan, idsSet, sample, threeJoinedSize)
      } else if (children.size == 2){

        val (left, leftIds, leftSample, leftSize) = children(0).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, twoJoinedSize, threeJoinedSize)
        val (right, rightIds, rightSample, rightSize) = children(1).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, twoJoinedSize, threeJoinedSize)

        val condition = keysEachCondition.flatMap {
          case ((l, r), (lk, rk)) =>
            if (leftIds.contains(l) && rightIds.contains(r)) {
              val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
              Some(cond)
            }
            else None
        }.reduce((l, r) => And(l, r))

        val plan = TempJoin(left, right, Inner, Some(condition))
        val condMap = Map[(Int, Int), Option[Expression]]((0,1) -> Some(condition))
        val sample = if (useSample) {
          Some(joinSizeEstimator.joinSample(condMap, Seq[SampleStat[Any]](leftSample.get, rightSample.get)))
        } else {
          None
        }
        (plan, leftIds union rightIds, sample, twoJoinedSize)
      } else {
        children(0).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, partitionNum, twoJoinedSize, threeJoinedSize)
      }
    }

  }
  def treeToLogicalPlanWithSample(useSample: Boolean,
                                  relations: Seq[LogicalPlan],
                                  keysEachCondition: ConditionMatrix,
                                  catalog: MjSessionCatalog,
                                  oneRoundStrategy: OneRoundStrategy,
                                  multiRoundStrategy: MultiRoundStrategy,
                                  joinSizeEstimator: JoinSizeEstimator,
                                  confJoinNode: MultiTree[ConfigTreeNode],
                                  partitionNum: Int = 100,
                                  scale: Int = 10000): (LogicalPlan, Set[Int], Option[SampleStat[Any]], Long) = {
    if (this.children == null || this.children.isEmpty) {
      val plan = relations(this.v)
      val statistics = catalog.getStatistics(plan)
      val sample: Option[SampleStat[Any]] = if (useSample) {
        statistics.map(_.extractSample)
      } else None
//      val size = statistics.map(_.rowCount.get.toLong / scale).reduce(_ * _) * scale
      val count = statistics.get.rowCount.get.longValue()
      (plan, Set[Int](this.v),sample, count)
    } else {
      if (this.children.size >= 3) {

        val childrenOutputs = children.map(_.output(relations))
        val confChildrenOutput = confJoinNode.children.map(_.v.output.sorted)
        val confChildren = confJoinNode.children
        // 根据配置信息对子节点进行匹配, 因为语法树子节点的构造顺序可能与配置不同
        val orderedChildren = childrenOutputs.map {
          case attrs =>
            val names = attrs.map(_.toString()).sorted.map(_.split('#').head)
            var s: MultiTree[ConfigTreeNode] = null
            var i = 0
            while (i < confChildrenOutput.length && s == null) {
              val confName = confChildrenOutput(i)
              if (names.zip(confName).forall {
                case (x, y) =>
                  x == y
              }) s = confChildren(i)
              i += 1
            }
            assert(s != null, s"s is null, names:$names, confOutput: ${confChildrenOutput.map(_.mkString("[",",","]")).mkString("-")}")
            s
        }

        val planWithSamples = this.children.zip(orderedChildren).map {
          case (c, conf) =>
            c.treeToLogicalPlanWithSample(true, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator,conf, partitionNum, scale)
        }

        val idsEachChild = planWithSamples.map(_._2)
        val samples = planWithSamples.map(_._3).map(_.get)
        val plans = planWithSamples.map(_._1)
        val idsSet = idsEachChild.fold(Set[Int]())(_ ++ _)

        // rId -> childId
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

        val condMap = joinConditions.map {
          case (ids, (lk, rk)) =>
            val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
            ids -> Some(cond)
        }
        val multiRoundSample = joinSizeEstimator.joinSample(condMap, samples)
        val sample = if (useSample) Some(multiRoundSample) else None

        if (confJoinNode.v.joinType.equals("Share")) {
          val sizes = confJoinNode.children.map(_.v.size)
          oneRoundStrategy.loadArgument(joinConditions, plans, sizes, samples, partitionNum)
          val plan = oneRoundStrategy.optimize()
          (plan, idsSet, sample, 1000)
        } else {
          val plan = multiRoundStrategy.optimize(joinConditions, plans)
          (plan, idsSet, sample, 1000)
        }
      } else if (children.size == 2){
        assert(confJoinNode.children.length == 2, s"conf len is ${confJoinNode.children.length}, not 2")
        val childrenOutputs = children.map(_.output(relations))
        val confChildrenOutput = confJoinNode.children.map(_.v.output.sorted)
        val confChildren = confJoinNode.children
        // 根据配置信息对子节点进行匹配, 因为语法树子节点的构造顺序可能与配置不同
        val orderedChildren = childrenOutputs.map {
          case attrs =>
            val names = attrs.map(_.toString()).sorted.map(_.split('#').head)
            var s: MultiTree[ConfigTreeNode] = null
            var i = 0
            while (i < confChildrenOutput.length && s == null) {
              val confName = confChildrenOutput(i)
              if (names.zip(confName).forall {
                case (x, y) =>
                  x == y
              }) s = confChildren(i)
              i += 1
            }
            assert(s != null, s"s is null, names:$names")
            s
        }

        val (left, leftIds, leftSample, leftSize) = children(0).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, orderedChildren(0), partitionNum, scale)
        val (right, rightIds, rightSample, rightSize) = children(1).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, orderedChildren(1), partitionNum, scale)

        val condition = keysEachCondition.flatMap {
          case ((l, r), (lk, rk)) =>
            if (leftIds.contains(l) && rightIds.contains(r)) {
              val cond = lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression]).reduce((l, r) => And(l, r))
              Some(cond)
            }
            else None
        }.reduce((l, r) => And(l, r))

        val plan = TempJoin(left, right, Inner, Some(condition))
        val condMap = Map[(Int, Int), Option[Expression]]((0,1) -> Some(condition))
        val sample = if (useSample) {
          Some(joinSizeEstimator.joinSample(condMap, Seq[SampleStat[Any]](leftSample.get, rightSample.get)))
        } else {
          None
        }
        (plan, leftIds union rightIds, sample, 1000)
      } else {
        children(0).treeToLogicalPlanWithSample(useSample, relations, keysEachCondition, catalog, oneRoundStrategy, multiRoundStrategy, joinSizeEstimator, confJoinNode, partitionNum, scale)
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
