package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.IntegerType

/**
 * Created by wuxiaoqi on 17-12-11.
 */
trait ArgumentsSet {
  val triangleData: Arguments = {
    val attributes: Map[String, AttributeReference] = Map[String, AttributeReference] (
      "a.x"->AttributeReference("x", IntegerType)(exprId = ExprId(1)),
      "b.x"->AttributeReference("x", IntegerType)(exprId = ExprId(2)),
      "b.y"->AttributeReference("y", IntegerType)(exprId = ExprId(3)),
      "c.y"->AttributeReference("y", IntegerType)(exprId = ExprId(4)),
      "a.z"->AttributeReference("z", IntegerType)(exprId = ExprId(5)),
      "c.z"->AttributeReference("z", IntegerType)(exprId = ExprId(6)))
    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("a.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (0, 2)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("c.z")))
    )
    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      LocalRelation(attributes("a.x"), attributes("a.z")),
      LocalRelation(attributes("b.x"), attributes("b.y")),
      LocalRelation(attributes("c.y"), attributes("c.z")))
    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x"), attributes("a.z")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z"))
    )
    Arguments(keys, joinConditions, relations)
  }
  val cliqueData: Arguments = {
    val attributes: Map[String, AttributeReference] = Map[String, AttributeReference] (
      "a.x"->AttributeReference("x", IntegerType)(exprId = ExprId(1)),
      "b.x"->AttributeReference("x", IntegerType)(exprId = ExprId(2)),
      "b.y"->AttributeReference("y", IntegerType)(exprId = ExprId(3)),
      "c.y"->AttributeReference("y", IntegerType)(exprId = ExprId(4)),
      "a.z"->AttributeReference("z", IntegerType)(exprId = ExprId(5)),
      "c.z"->AttributeReference("z", IntegerType)(exprId = ExprId(6)),
      "d.s"->AttributeReference("s", IntegerType)(exprId = ExprId(7)),
      "e.s"->AttributeReference("s", IntegerType)(exprId = ExprId(8)),
      "f.s"->AttributeReference("s", IntegerType)(exprId = ExprId(9)),
      "d.z"->AttributeReference("z", IntegerType)(exprId = ExprId(10)),
      "e.x"->AttributeReference("x", IntegerType)(exprId = ExprId(11)),
      "f.y"->AttributeReference("y", IntegerType)(exprId = ExprId(12)))

    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("a.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (0, 2)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("c.z"))),
      (3, 4)->(Seq[Expression](attributes("d.s")), Seq[Expression](attributes("e.s"))),
      (4, 5)->(Seq[Expression](attributes("e.s")), Seq[Expression](attributes("f.s"))),
      (3, 2)->(Seq[Expression](attributes("d.z")), Seq[Expression](attributes("c.z"))),
      (4, 0)->(Seq[Expression](attributes("e.x")), Seq[Expression](attributes("a.x"))),
      (5, 1)->(Seq[Expression](attributes("f.y")), Seq[Expression](attributes("b.y")))
    )
    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      LocalRelation(attributes("a.x"), attributes("a.z")),
      LocalRelation(attributes("b.x"), attributes("b.y")),
      LocalRelation(attributes("c.y"), attributes("c.z")),
      LocalRelation(attributes("d.s"), attributes("d.z")),
      LocalRelation(attributes("e.s"), attributes("e.x")),
      LocalRelation(attributes("f.s"), attributes("f.y")))

    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x"), attributes("a.z")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z")),
      Seq[Expression](attributes("d.s"), attributes("d.z")),
      Seq[Expression](attributes("e.s"), attributes("e.x")),
      Seq[Expression](attributes("f.s"), attributes("f.y"))
    )
    Arguments(keys, joinConditions, relations)
  }
  val lineData: Arguments = {
    val attributes: Map[String, AttributeReference] = Map[String, AttributeReference] (
      "a.x"->AttributeReference("x", IntegerType)(exprId = ExprId(1)),
      "b.x"->AttributeReference("x", IntegerType)(exprId = ExprId(2)),
      "b.y"->AttributeReference("y", IntegerType)(exprId = ExprId(3)),
      "c.y"->AttributeReference("y", IntegerType)(exprId = ExprId(4)),
      "a.z"->AttributeReference("z", IntegerType)(exprId = ExprId(5)),
      "d.z"->AttributeReference("z", IntegerType)(exprId = ExprId(6)))
    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("a.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (2, 3)->(Seq[Expression](attributes("c.z")), Seq[Expression](attributes("d.z")))
    )
    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      LocalRelation(attributes("a.x")),
      LocalRelation(attributes("b.x"), attributes("b.y")),
      LocalRelation(attributes("c.y"), attributes("c.z")),
      LocalRelation(attributes("d.z")))
    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z")),
      Seq[Expression](attributes("d.z"))
    )
    Arguments(keys, joinConditions, relations)
  }
  val arbitraryData: Arguments = {
    val attributes: Map[String, AttributeReference] = Map[String, AttributeReference] (
      "a.x"->AttributeReference("x", IntegerType)(exprId = ExprId(1)),
      "b.x"->AttributeReference("x", IntegerType)(exprId = ExprId(2)),
      "b.y"->AttributeReference("y", IntegerType)(exprId = ExprId(3)),
      "c.y"->AttributeReference("y", IntegerType)(exprId = ExprId(4)),
      "a.z"->AttributeReference("z", IntegerType)(exprId = ExprId(5)),
      "c.z"->AttributeReference("z", IntegerType)(exprId = ExprId(6)),
      "d.z"->AttributeReference("z", IntegerType)(exprId = ExprId(7)),
      "d.p"->AttributeReference("p", IntegerType)(exprId = ExprId(8)),
      "e.p"->AttributeReference("p", IntegerType)(exprId = ExprId(9)),
      "e.q"->AttributeReference("q", IntegerType)(exprId = ExprId(10)),
      "f.q"->AttributeReference("q", IntegerType)(exprId = ExprId(11)),
      "f.s"->AttributeReference("s", IntegerType)(exprId = ExprId(12)),
      "g.s"->AttributeReference("s", IntegerType)(exprId = ExprId(13)),
      "h.y"->AttributeReference("y", IntegerType)(exprId = ExprId(14)),
      "h.r"->AttributeReference("r", IntegerType)(exprId = ExprId(15)),
      "i.r"->AttributeReference("r", IntegerType)(exprId = ExprId(16)),
      "i.w"->AttributeReference("w", IntegerType)(exprId = ExprId(17)),
      "j.w"->AttributeReference("w", IntegerType)(exprId = ExprId(18)))

    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("a.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (0, 2)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("c.z"))),
      (0, 3)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("d.z"))),
      (3, 4)->(Seq[Expression](attributes("d.p")), Seq[Expression](attributes("e.p"))),
      (4, 5)->(Seq[Expression](attributes("e.q")), Seq[Expression](attributes("f.q"))),
      (5, 6)->(Seq[Expression](attributes("f.s")), Seq[Expression](attributes("g.s"))),
      (1, 7)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("h.y"))),
      (7, 8)->(Seq[Expression](attributes("h.r")), Seq[Expression](attributes("i.r"))),
      (8, 9)->(Seq[Expression](attributes("i.w")), Seq[Expression](attributes("j.w")))
    )
    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      LocalRelation(attributes("a.x"), attributes("a.z")),
      LocalRelation(attributes("b.x"), attributes("b.y")),
      LocalRelation(attributes("c.y"), attributes("c.z")),
      LocalRelation(attributes("d.z"), attributes("d.p")),
      LocalRelation(attributes("e.p"), attributes("e.q")),
      LocalRelation(attributes("f.q"), attributes("f.s")),
      LocalRelation(attributes("g.s")),
      LocalRelation(attributes("h.y"), attributes("h.r")),
      LocalRelation(attributes("i.r"), attributes("i.w")),
      LocalRelation(attributes("j.w")))
    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x"), attributes("a.z")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z")),
      Seq[Expression](attributes("d.z"), attributes("d.p")),
      Seq[Expression](attributes("e.p"), attributes("e.q")),
      Seq[Expression](attributes("f.q"), attributes("f.s")),
      Seq[Expression](attributes("g.s")),
      Seq[Expression](attributes("h.y"), attributes("h.r")),
      Seq[Expression](attributes("i.r"), attributes("i.w")),
      Seq[Expression](attributes("j.w"))
    )
    Arguments(keys, joinConditions, relations)
  }
}
