package org.pasalab.automj

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Coalesce, EqualNullSafe, EqualTo, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}

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
  type ConditionMatrix = mutable.Map[(Int, Int), JoinKeys]

  // 1. The first arg is an array, the index of which is relation id, and the value is keys.
  // 2. (left relation id, right relation id) -> join condition.
  // 3. Other conditions.
  // 4. All relations, the index of the array is relation id.
  type ReturnType = (Seq[Seq[Expression]],
    ConditionMatrix,
    Seq[Expression],
    Seq[LogicalPlan])

  type MutableReturnType = (
    mutable.ArraySeq[Seq[Expression]],
      ConditionMatrix,
      Seq[Expression],
      Seq[LogicalPlan])

  def combineSeqTuple(x: JoinKeys, y: JoinKeys): JoinKeys = {
    (x._1 ++ y._1, x._2 ++ y._2)
  }

  /**
   * This function handle the problem where both children have same relations. It mainly
   * combine left children with right children, and combine corresponding keys set. Added by
   * xiaoqi wu.
   * e.g.
   *
   *       inner Join
   *         /   \
   * inner Join  inner Join
   *       /   \    /   \
   * child1   child2   child3
   *
   * @param lk keys each left child relation
   * @param rk keys each right child relation
   * @param l  left child relation
   * @param r  right child relation
   * @return result after combining
   */
  def combineKeysAndRalations(
                               lk: mutable.ArraySeq[Seq[Expression]],
                               rk: mutable.ArraySeq[Seq[Expression]],
                               l: Seq[LogicalPlan],
                               r: Seq[LogicalPlan]): (mutable.ArraySeq[Seq[Expression]], Seq[LogicalPlan], Map[Int, Int]) = {

    val overlap = l.toSet.intersect(r.toSet)
    if (overlap.isEmpty) {
      val map = (0 until r.length).map(x => (x, x + l.length)).toMap
      (lk ++ rk, l ++ r, map)
    } else {
      val resultKeys = lk.clone()
      val leftOverlap = l.zipWithIndex.filter(x => overlap.contains(x._1))
      val rightWithIndex = r.zipWithIndex
      val (rightOverlap, rightAddable) = rightWithIndex.partition(x => overlap.contains(x._1))
      val tmpKeys = rk.zip(r).filterNot(x => overlap.contains(x._2)).unzip._1

      var i = 0
      while (i < overlap.size) {
        val leftIndex = leftOverlap(i)._2
        val rightIndex = rightOverlap.find(_._1 eq l(leftIndex)).get._2
        resultKeys(leftIndex) = resultKeys(leftIndex) ++ rk(rightIndex)
        i += 1
      }

      val leftTmpMap = leftOverlap.toMap
      val rightTmpMap = rightOverlap.toMap
      val overlapMap = overlap.map(x => (rightTmpMap.getOrElse(x, -1),
        leftTmpMap.getOrElse(x, -1)))
      val map = (rightAddable.zipWithIndex
        .map(x => (x._1._2, x._2 + l.length)) ++
        overlapMap)
        .toMap

      logInfo("[OVERLAP]!!!!!!!")
      (resultKeys ++ tmpKeys, l ++ rightAddable.unzip._1, map)
    }
  }
  /** Its function is combining 2 matrix like matrix adding. */
  def combineMaps(x: ConditionMatrix,
                  y: ConditionMatrix,
                  map: Map[Int, Int]): ConditionMatrix = {
    val m = x.clone()
    val o = y.clone().map{
      case (k, v) => (map(k._1), map(k._2)) -> v
    }
    val overlap = m.keySet.intersect(o.keySet)

    if (overlap.isEmpty) {
      m ++ o
    } else {
      overlap.foreach(x => o(x) = combineSeqTuple(o.getOrElse(x, (Seq(), Seq())),
        m.getOrElse(x, (Seq(), Seq()))
      ))
      m ++ o
    }
  }
  /** flatten all inner joins, which are next to each other. */
  // TODO: Test the correctness of the method. 16-12-9.
  def flattenJoin(plan: LogicalPlan): MutableReturnType = plan match {
    case Join(left, right, Inner, condition) =>
      val (leftKeysEachRelation, leftBothKeysConds, leftConds, leftPlans) = flattenJoin(left)
      val (rightKeysEachRelation, rightBothKeysConds, rightConds, rightPlans) = flattenJoin(right)


      val (keysEachRelation, relations, map) = combineKeysAndRalations(leftKeysEachRelation,
        rightKeysEachRelation, leftPlans, rightPlans)
      val combineBothKeysConds = combineMaps(leftBothKeysConds, rightBothKeysConds, map)

      if (keysEachRelation.length != relations.length) {
        throw new RuntimeException(s"The length of KeysEachRelation does not match" +
          s"Relations (${keysEachRelation.length}, ${relations.length})")
      }

      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      var pid = 0
      var hasChanged = false
//      val addableKeys: MutablePair[Expression, Expression] = new MutablePair()
      while (pid < predicates.length) {
        var addableLeftKey: Expression = null
        var addableRightKey: Expression = null
        var leftIndex = -1
        var rightIndex = -1
        predicates(pid) match {
          case EqualTo(l, r) =>
            hasChanged = true
            var i = 0
            while (i < relations.length) {
              relations(i) match {
                case relation if canEvaluate(l, relation) =>
                  if (!keysEachRelation(i).exists(_.semanticEquals(l))) {
                    keysEachRelation(i) = keysEachRelation(i) :+ l
                  }

                  leftIndex = i
                  addableLeftKey = l
                  if (rightIndex > -1) i = relations.length
                  else i += 1
                case relation if canEvaluate(r, relation) =>
                  if (!keysEachRelation(i).exists(_.semanticEquals(r))) {
                    keysEachRelation(i) = keysEachRelation(i) :+ r
                  }
                  rightIndex = i
                  addableRightKey = r
                  if (leftIndex > -1) i = relations.length
                  else i += 1
                case _ => i += 1
              }
            }

          case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
            hasChanged = true
            var i = 0
            while (i < relations.length) {
              relations(i) match {
                case relation if canEvaluate(l, relation) =>
                  val nullSafeLeftKey = Coalesce(Seq(l, Literal.default(l.dataType)))
                  if (!keysEachRelation(i).exists(_.semanticEquals(nullSafeLeftKey))) {
                    keysEachRelation(i) = keysEachRelation(i) :+ nullSafeLeftKey
                  }

                  leftIndex = i
                  addableLeftKey = l
                  if (rightIndex > -1) i = relations.length
                  else i += 1
                case relation if canEvaluate(r, relation) =>
                  val nullSafeRightKey = Coalesce(Seq(r, Literal.default(r.dataType)))
                  if (!keysEachRelation(i).exists(_.semanticEquals(nullSafeRightKey))) {
                    keysEachRelation(i) = keysEachRelation(i) :+ nullSafeRightKey
                  }
                  rightIndex = i
                  addableRightKey = r
                  if (leftIndex > -1) i = relations.length
                  else i += 1
                case _ => i += 1
              }
            }
          case _ =>
        }

        if (leftIndex != -1 && rightIndex != -1) {
          val k = (leftIndex, rightIndex)
          if (combineBothKeysConds.contains(k)) {
            val v = (combineBothKeysConds(k)._1 :+ addableLeftKey,
              combineBothKeysConds(k)._2 :+ addableRightKey)
            combineBothKeysConds.put(k, v)
          } else {
            combineBothKeysConds += k -> (Seq(addableLeftKey), Seq(addableRightKey))
          }
        }

        pid += 1
      }

      val otherPredicates = predicates.filterNot {
        case EqualTo(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case other => false
      }

      hasChanged match {
        case true => (keysEachRelation,
          combineBothKeysConds,
          leftConds ++ rightConds ++ otherPredicates,
          relations)
        case _ => (mutable.ArraySeq(Seq()), mutable.Map(), Seq(), Seq(plan))
      }
    case Filter(filterCondition, j @ Join(left, right, Inner, joinCondition)) =>
      val (keys, condKeys, conds, childplans) = flattenJoin(j)
      (keys, condKeys, conds ++ splitConjunctivePredicates(filterCondition), childplans)

    case Project(projectList, j @ Join(left, right, Inner, joinCondition)) =>
      logInfo(s"terminate pattern where plan is Project(_, Join)")
      (mutable.ArraySeq(Seq()), mutable.Map(), Seq(), Seq(plan))
    case _ =>
      logInfo(s"terminate pattern where plan is ${plan.nodeName}")
      (mutable.ArraySeq(Seq()), mutable.Map(), Seq(), Seq(plan))
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case j @ Join(_, _, Inner, _) =>
      Some(flattenJoin(j))
    case _ => None
  }
}
