package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj._

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-11-29.
 */
case class MjOptimizer(oneRoundStrategy: Option[OneRoundStrategy] = None,
                       multiRoundStrategy: Option[MultiRoundStrategy] = None,
                       joinSizeEstimator: Option[JoinSizeEstimator] = None,
                       forceOneRound: Boolean, sqlConf: SQLConf) extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (oneRoundStrategy.isDefined && multiRoundStrategy.isDefined && joinSizeEstimator.isDefined) {
      val oneRoundCore = oneRoundStrategy.get
      val multiRoundCore = multiRoundStrategy.get
      val joinSizeEstimatorCore = joinSizeEstimator.get

      val mode: String = sqlConf.getConfString(MjConfigConst.EXECUTION_MODE)

      val r = mode match {
        case "default" =>
          plan
        case "one-round" =>
          oneRoundMode(oneRoundCore)(plan)
        case "mixed" =>
          mixedMode(oneRoundCore, multiRoundCore, joinSizeEstimatorCore, forceOneRound)(plan)
      }

      r
    } else plan
  }

  def mixedMode(oneRoundCore: OneRoundStrategy,
                multiRoundCore: MultiRoundStrategy,
                joinSizeEstimatorCore: JoinSizeEstimator,
                forceOneRound: Boolean)(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case MjExtractor(keysEachRelation,
      originBothKeysEachCondition, otherConditions, relations)
        if (sqlConf.getConfString(MjConfigConst.ONE_ROUND_ONCE, "false") == "true" ) =>
//        assert(false, s"keys: ${keysEachRelation.map(_.mkString(",")).mkString("-")} \n" +
//          s"joins: ${originBothKeysEachCondition.map {
//            case ((l, r), (lk, rk)) =>
//              s"($l, $r)->[(${lk.mkString(",")}), (${rk.mkString(",")})]"
//          }.mkString("\n")}\n" +
//          s"other: ${otherConditions.mkString(",")}")
        // 这个图适用于划分等价类
        val initEdges = originBothKeysEachCondition.toSeq.map {
          case ((l, r), (lk, rk)) =>
            (AttributeVertex(l, lk), AttributeVertex(r, rk))
        }
        val equivalenceClasses = Graph(initEdges).connectComponent()
        val edges = mutable.HashMap[(Int, Int),Set[Int]]()
        val len = equivalenceClasses.length
        for (v <- 0 to len - 1; u <- v + 1 to len - 1) {
          val vTableIds = equivalenceClasses(v).map(_.v.rId).toSet
          val uTableIds = equivalenceClasses(u).map(_.v.rId).toSet
          val ints = vTableIds.intersect(uTableIds)
          if (ints.nonEmpty) {
            edges += (v, u) -> ints
          }
        }
        // 找出查询结构中的等价类环
        val circle = findCircle(edges.toSeq.map(_._1), relations.length)
        val rIdsInCircle: Seq[Int] = {
          val tIdsInCircle = mutable.Set[Int]()
          for (((a, b), tIds) <- edges) {
            if (circle.contains(a) && circle.contains(b)) {
              assert(tIds.nonEmpty, "two equivalence classes has no common tables")
              tIdsInCircle ++= tIds
            }
          }
          tIdsInCircle.toSeq
        }

        val (tmpOneRoundCondition, tmpCondition) = originBothKeysEachCondition.partition {
          case ((l, r), _) =>
            rIdsInCircle.contains(l) && rIdsInCircle.contains(r)
        }
        if (rIdsInCircle.nonEmpty) assert(tmpOneRoundCondition.nonEmpty, s"rIdsInCircle is ${rIdsInCircle.mkString(",")}")
        val keys = originBothKeysEachCondition.flatMap(x => x._2._1 ++ x._2._1).toSet
        val (oneRoundKeys, oneRoundRelations) = rIdsInCircle
          .map(rId => (keysEachRelation(rId).filter(keys), relations(rId))).unzip
        val idMap = rIdsInCircle.zipWithIndex.toMap
        //          assert(false, s"idMap: ${idMap.map {case (k, v) => s"$k -> $v"}.mkString("\n")}")
        val oneRoundJoins = tmpOneRoundCondition.map {
          case ((l, r), v) =>
            assert(idMap.contains(l), s"idMap: ${idMap.map { case (k, v) => s"$k -> $v" }.mkString("\n")}")
            assert(idMap.contains(r), s"idMap: ${idMap.map { case (k, v) => s"$k -> $v" }.mkString("\n")}")
            ((idMap(l), idMap(r)), v)
        }

        val useOneRound: Boolean = if (oneRoundRelations.nonEmpty) {
          oneRoundCore.refresh(oneRoundKeys, oneRoundJoins, oneRoundRelations,
            sqlConf.getConfString(MjConfigConst.ONE_ROUND_PARTITIONS, "100").toInt)
          joinSizeEstimatorCore.refresh(oneRoundJoins, oneRoundRelations)
          forceOneRound || oneRoundCore.cost() < joinSizeEstimatorCore.cost()
        } else {
          false
        }

        // 如果OneRound策略的通信量小，那么就对剩余的Join条件进行划分，分出哪些用于链接OneRound，哪些用于MultiRound内部
        val (combinedConditionMap, multiRoundCondition) = if (useOneRound) {
          tmpCondition.partition {
            case ((l, r), _) =>
              rIdsInCircle.contains(l) || rIdsInCircle.contains(r)
          }
        } else {
          (mutable.Map[(Int, Int), (Seq[Expression], Seq[Expression])](), originBothKeysEachCondition)
        }


        // 生成一轮Join的LogicalPlan ShareJoin
        val multiRoundIds = (0 to relations.length - 1).filter(i => !rIdsInCircle.contains(i))

        // 将剩余的relations按照Join条件组织成一张图, 凡是有Join关系的表就会相连, 进行等价类划分
        // 然后把与ShareJoin相连的condition也按照等价类划分出来
        // 每个branch表示(join分支, 和shareJoin连接的条件)
        // 如果没有环, 那么branches就是长度就为1
        val branches = if (multiRoundCondition.nonEmpty) {
          Graph(multiRoundCondition.toSeq.map(_._1)).connectComponent().map {
            case nodes =>
              val set = nodes.map(_.v).toSet
              val branchMap = multiRoundCondition.filter {
                case ((l, r), x) =>
                  set.contains(l) && set.contains(r)
              }.toMap

              // 当前分支涉及到的表
              val relationIds: Set[Int] = branchMap.toSeq.flatMap {
                case ((l, r), v) =>
                  Seq[Int](l, r)
              }.toSet
              // 当前分支和环结构连接的条件, combinedConditionMap每个条件最多只可能包含当前分支的一张表
              val combinedCondition: Option[Expression] = if (combinedConditionMap.nonEmpty) {
                Some(
                  combinedConditionMap
                    .filter {
                      case ((l, r), c) => relationIds.contains(l) || relationIds.contains(r)
                    }
                    .flatMap {
                      case ((l, r), (lk, rk)) =>
                        lk.zip(rk).map(x => EqualTo(x._1, x._2).asInstanceOf[Expression])
                    }.reduce((l, r) => And(l, r))
                )
              } else None

              (multiRoundCore.optimize(branchMap, relations), combinedCondition)
          }
        } else Nil

        // 合并一轮Join节点和多轮Join的节点
        val j: LogicalPlan = if (useOneRound) {
          //            assert(branches.length > 0, "when use one round strategy, the plan must have at least 1 branch")
          val oneRound: LogicalPlan = oneRoundCore.optimize()
          assert(oneRound.isInstanceOf[ShareJoin], s"not ShareJoin(${oneRound.getClass.getName})")
          branches.foldLeft(oneRound) {
            case (pre, branch) =>
              Join(pre, branch._1, Inner, branch._2)
          }
        } else {
          // 因为没有环，所以这张图是联通的
          assert(branches.length == 1, s"branches(${branches.length})")
          branches.head._1
        }
        sqlConf.setConfString(MjConfigConst.ONE_ROUND_ONCE, "false")

        // 如果多轮Join之后还有条件谓词，就加个过滤器
        //          assert(false, s"plan output: ${j.output.mkString(",")}, otherCondition: ${otherConditions.isEmpty}")
        if (otherConditions.isEmpty) Project(plan.output, j)
        else {
          val filterCondition = otherConditions.reduce((l, r) => And(l, r))
          Project(plan.output, Filter(filterCondition, j))
        }
    }
  }

  def oneRoundMode(oneRoundCore: OneRoundStrategy)(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case MjExtractor(keysEachRelation,
      originBothKeysEachCondition, otherConditions, relations)
        if (sqlConf.getConfString(MjConfigConst.ONE_ROUND_ONCE, "false") == "true" )=>
        //          assert(false, s"keys: ${keysEachRelation.map(_.mkString(",")).mkString("-")} \n" +
        //            s"joins: ${originBothKeysEachCondition.map {
        //              case ((l, r), (lk, rk)) =>
        //                s"($l, $r)->[(${lk.mkString(",")}), (${rk.mkString(",")})]"
        //            }.mkString("\n")}\n" +
        //            s"other: ${otherConditions.mkString(",")}")
        // 找出查询结构中的环
        oneRoundCore.refresh(keysEachRelation, originBothKeysEachCondition, relations,
          sqlConf.getConfString(MjConfigConst.ONE_ROUND_PARTITIONS, "100").toInt)

        // 合并一轮Join节点和多轮Join的节点
        val j: LogicalPlan = oneRoundCore.optimize()
        assert(j.isInstanceOf[ShareJoin], s"not ShareJoin(${j.getClass.getName})")

        sqlConf.setConfString(MjConfigConst.ONE_ROUND_ONCE, "false")
        // 如果多轮Join之后还有条件谓词，就加个过滤器
        if (otherConditions.isEmpty) Project(plan.output, j)
        else {
          val filterCondition = otherConditions.reduce((l, r) => And(l, r))
          Project(plan.output, Filter(filterCondition, j))
        }
    }
  }

  /**
   * 找到图中带有环的结构, 拓扑排序的算法可以保证一个连通图的带环的结构只有一个(即使有多个也会连在一起构成一个大的带环结构)
   * @param edges 边集
   * @param len 点的个数
   * @return 带环结构的所有点
   */
  def findCircle(edges: Seq[(Int, Int)], len: Int): Seq[Int] = {
    assert(edges.nonEmpty, "edges is empty")
    val neighbors: Map[Int, Seq[Int]] = edges.flatMap {
      case (l, r) =>
        Seq((l, r), (r, l))
    }.groupBy(_._1).map {
      case (k, v) => (k, v.map(_._2))
    }
    // 初始化每个顶点的度
    val degrees = new Array[Int](len)
    for (e <- edges) {
      degrees(e._1) += 1
      degrees(e._2) += 1
    }

    var alone = false
    while (!alone) {
      alone = true
      for (i <- 0 to len - 1 if degrees(i) == 1) {
        degrees(i) = 0
        alone = false
        for (neighbor <- neighbors(i) if degrees(neighbor) > 0) degrees(neighbor) -= 1
      }
    }
    val circle = (0 to len - 1).filter(i => degrees(i) > 0)
//    assert(circle.length > 2, s"edges: ${edges.map(x => s"(${x._1}, ${x._2})").mkString(",")}, circle: ${circle.mkString(",")}")
    circle
  }
}
