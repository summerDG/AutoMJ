package org.pasalab.automj

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}

import scala.collection.mutable

/**
 * Created on 2016/10/8. By xiaoqi wu.
 * This pattern is used to extract multi joins from logical plan tree.
 * A pattern following.
 *
 *        Equi-Join
 *   (plan0.a == plan2.a)
 *          /      \            ---->      (Seq(Seq(a,c), Seq(c), Seq(a,b), Seq(b)),
 *      Filter   Equi-Join                  Map((0, 1)->(plan0.c, plan1.c),...),
 * (condition0) (plan2.b == plan3.b)        Seq(condition0),
 *        |        /      \                 Seq(plan0, plan1, plan2, plan3))
 *    Equi-Join  plan2    plan3
 * (plan0.c == plan1.c)
 *      /      \
 *   plan0     plan1
 */
object MjExtractor extends Logging with PredicateHelper {

  // left expressions == right expression
  type JoinKeys = (Seq[Expression], Seq[Expression])
  // (left relation id, right relation id) -> join condition
  type ConditionMatrix = Map[(Int, Int), JoinKeys]

  // 1. The first arg is an array, the index of which is relation id, and the value is keys.
  // 2. (left relation id, right relation id) -> join condition.
  // 3. Other conditions.
  // 4. All relations, the index of the array is relation id.
  type ReturnType = (Seq[Seq[Expression]],
    ConditionMatrix,
    Seq[Expression],
    Seq[LogicalPlan])

  def splitJoin(plan: LogicalPlan): Option[ReturnType] = {
    val (items, conditions) = extractInnerJoins(plan)
    val otherConditions = mutable.ArrayBuffer[Expression]()
    if (items.size > 2 && conditions.nonEmpty) {
      val joins: ConditionMatrix = {
        val m = (0 to items.size - 1) flatMap {
          case i: Int =>
            (i + 1 to items.size - 1) flatMap {
              case j =>
                val onePlan = items(i)
                val otherPlan = items(j)
                val joinConds = conditions
                  .filterNot(l => canEvaluate(l, onePlan))
                  .filterNot(r => canEvaluate(r, otherPlan))
                  .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
                  .flatMap {
                    case EqualTo(l, r)=>
                      if (canEvaluate(l, onePlan) && canEvaluate(r, otherPlan)) {
                        Some((l, r))
                      } else if (canEvaluate(l, otherPlan) && canEvaluate(r, onePlan)){
                        Some((r, l))
                      } else None
                    case otherCondition => {
                      otherConditions += otherCondition
                      None
                    }
                  }
                if (joinConds.isEmpty) {
                  None
                } else {
                  val k = (i, j)
                  Some((k -> joinConds.toSeq.unzip))
                }
            }
        }
        m.toMap
      }
      val keys = new Array[Seq[Expression]](items.size).map(_ => Seq[Expression]())
      for (((l, r), (lk, rk)) <- joins) {
        keys(l) ++= {
          val covered = keys(l).find(old => lk.find(k => k.semanticEquals(old)).isDefined)
          covered match {
            case None => lk
            case _ => Seq[Expression]()
          }
        }
        keys(r) ++= {
          val covered = keys(r).find(old => rk.find(k => k.semanticEquals(old)).isDefined)
          covered match {
            case None => rk
            case _ => Seq[Expression]()
          }
        }
      }
      Some((keys.toSeq, joins, otherConditions, items))
    } else {
      None
    }
  }

  def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Set[Expression]) = {
    plan match {
      case Join(left, right, _: InnerLike, Some(cond)) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, splitConjunctivePredicates(cond).toSet ++
          leftConditions ++ rightConditions)
      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond)))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), Set())
    }
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case j @ Join(_, _, _: InnerLike, Some(cond)) =>
      splitJoin(j)
    case p @ Project(projectList, Join(_, _, _: InnerLike, Some(cond)))
      if projectList.forall(_.isInstanceOf[Attribute]) =>
      splitJoin(p)
    case other => None
  }
}
