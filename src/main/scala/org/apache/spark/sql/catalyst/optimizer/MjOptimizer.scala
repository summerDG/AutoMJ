package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, ShareJoin}
import org.apache.spark.sql.catalyst.rules.Rule
import org.pasalab.automj._

/**
 * Created by wuxiaoqi on 17-11-29.
 */
case class MjOptimizer(strategy: ReduceTreeStrategy) extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case MjExtractor(keysEachRelation,
      originBothKeysEachCondition, otherConditions, relations) =>
        // 找出查询结构中的环
        val circle: Seq[Int] = findCircle(originBothKeysEachCondition.map(_._1).toSeq, relations.length)
        //TODO: 比较这个circle用一轮Join和多轮Join的通信量...
        //TODO: 把originBothKeysEachCondition分成两部分, 一部分是一轮Join, 另一部分是多轮Join
        val (tmpOneRoundCondition, tmpCondition) = originBothKeysEachCondition.partition {
          case ((l, r), _) =>
            circle.contains(l) && circle.contains(r)
        }
        val (combinedConditionMap, multiRoundCondition) = tmpCondition.partition {
          case ((l, r), _) =>
            circle.contains(l) || circle.contains(r)
        }

        val multiRoundIds = (0 to relations.length - 1).filter(i => !circle.contains(i))
        // 将剩余的relations按照Join条件组织成一张图, 凡是有Join关系的表就会相连, 进行等价类划分
        // 然后把与ShareJoin相连的condition也按照等价类划分出来
        // 每个branch表示(join分支, 和shareJoin连接的条件)
        val branches = Graph(multiRoundCondition.map(_._1).toSeq).connectComponent().map {
          case nodes =>
            val set = nodes.map(_.v).toSet
            val branchMap = multiRoundCondition.filter {
              case ((l,r),x) =>
                set.contains(l) && set.contains(r)
            }.toMap
            val combinedCondition: Expression = combinedConditionMap.flatMap {
              case ((l, r), (lk,rk)) =>
                if (multiRoundIds.contains(l) || multiRoundIds.contains(r))
                  Some(lk.zip(rk).map(x => EqualTo(x._1, x._2)).reduce((l,r) => And(l, r)))
                else None
            }.reduce((l,r) => And(l, r))
            (strategy.reduce(branchMap, relations), combinedCondition)
        }

//        val hypergraph = HyperGraph(multiRoundCondition.toMap, relations, multiRoundIds)

        // 生成一轮Join的LogicalPlan ShareJoin
        val keys = originBothKeysEachCondition.flatMap(x => x._2._1 ++ x._2._1).toSet
        val (oneRoundKeys, oneRoundRelations) = circle
          .map(rId => (keysEachRelation(rId).filter(keys), relations(rId))).unzip
        val idMap = circle.zipWithIndex.toMap
        val oneRoundCondition = tmpOneRoundCondition.map {
          case ((l, r), v) =>
            ((idMap(l), idMap(r)), v)
        }.toMap
        val initEdges = oneRoundCondition.map {
          case ((l, r), (lk, rk)) =>
            (AttributeVertex(idMap(l), lk), AttributeVertex(idMap(r), rk))
        }.toSeq

        val oneRound: LogicalPlan = ShareJoin(oneRoundKeys, oneRoundCondition, None,
          oneRoundRelations, 1024, Graph(initEdges).connectComponent())

        // 合并一轮Join节点和多轮Join的节点
        val j: LogicalPlan = branches.foldLeft(oneRound) {
          case (pre, branch) =>
            Join(pre, branch._1, Inner, Some(branch._2))
        }

        // 如果多轮Join之后还有条件谓词，就加个过滤器
        if (otherConditions.isEmpty) j
        else {
          val filterCondition = otherConditions.reduce((l, r) => And(l, r))
          Filter(filterCondition, j)
        }
    }
  }
  def findCircle(edges: Seq[(Int, Int)], len: Int): Seq[Int] = {
    // 初始化每个顶点的度
    val degrees = new Array[Int](len)
    for (e <- edges) {
      degrees(e._1) += 1
      degrees(e._2) += 1
    }

    var alone = false
    while (!alone) {
      alone = true
      for (i <- 0 to len - 1 if degrees(i) > 0) {
        if (degrees(i) == 1) {
          alone = false
        }
        degrees(i) -= 1
      }
    }
    (0 to len - 1).filter(i => degrees(i) > 0)
  }
}
