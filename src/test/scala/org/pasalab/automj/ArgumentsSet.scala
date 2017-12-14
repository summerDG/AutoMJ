package org.pasalab.automj

import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-11.
 */
trait ArgumentsSet {self =>
  protected def spark: SparkSession
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }
  import internalImplicits._
  import ArgumentsSet._

  //TODO: 不应该在这里createView, 应该再原始的DataFrame上生成View
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
    val aDF = spark.sparkContext.parallelize(
        XzData(1, 1) ::
        XzData(1, 2) ::
        XzData(2, 1) ::
        XzData(2, 2) ::
        XzData(3, 1) ::
        XzData(3, 2) :: Nil, 2).toDF()
    aDF.createOrReplaceTempView("a")

    val bDF = spark.sparkContext.parallelize(
        XyData(1, 1) ::
        XyData(1, 2) ::
        XyData(2, 1) ::
        XyData(2, 2) ::
        XyData(3, 1) ::
        XyData(3, 2) :: Nil, 2).toDF()
    bDF.createOrReplaceTempView("b")

    val cDF = spark.sparkContext.parallelize(
        YzData(1, 1) ::
        YzData(1, 2) ::
        YzData(2, 1) ::
        YzData(2, 2) ::
        YzData(3, 1) ::
        YzData(3, 2) :: Nil, 2).toDF()
    cDF.createOrReplaceTempView("c")

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1)
    )
    Arguments(attributes, keys, joinConditions, relations, info)
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

    val aDF = spark.sparkContext.parallelize(
      XzData(1, 1) ::
        XzData(1, 2) ::
        XzData(2, 1) ::
        XzData(2, 2) ::
        XzData(3, 1) ::
        XzData(3, 2) :: Nil, 2).toDF()
    aDF.createOrReplaceTempView("a")

    val bDF = spark.sparkContext.parallelize(
      XyData(1, 1) ::
        XyData(1, 2) ::
        XyData(2, 1) ::
        XyData(2, 2) ::
        XyData(3, 1) ::
        XyData(3, 2) :: Nil, 2).toDF()
    bDF.createOrReplaceTempView("b")

    val cDF = spark.sparkContext.parallelize(
      YzData(1, 1) ::
        YzData(1, 2) ::
        YzData(2, 1) ::
        YzData(2, 2) ::
        YzData(3, 1) ::
        YzData(3, 2) :: Nil, 2).toDF()
    cDF.createOrReplaceTempView("c")

    val dDF = spark.sparkContext.parallelize(
      SzData(1, 1) ::
        SzData(1, 2) ::
        SzData(2, 1) ::
        SzData(2, 2) ::
        SzData(3, 1) ::
        SzData(3, 2) :: Nil, 2).toDF()
    dDF.createOrReplaceTempView("d")

    val eDF = spark.sparkContext.parallelize(
      SxData(1, 1) ::
        SxData(1, 2) ::
        SxData(2, 1) ::
        SxData(2, 2) ::
        SxData(3, 1) ::
        SxData(3, 2) :: Nil, 2).toDF()
    eDF.createOrReplaceTempView("e")

    val fDF = spark.sparkContext.parallelize(
      SyData(1, 1) ::
        SyData(1, 2) ::
        SyData(2, 1) ::
        SyData(2, 2) ::
        SyData(3, 1) ::
        SyData(3, 2) :: Nil, 2).toDF()
    fDF.createOrReplaceTempView("f")

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("d", 10, 10, Map[String, Long]("s"->11, "z"->9), dDF, 1),
      TableInfo("e", 10, 10, Map[String, Long]("s"->12, "x"->7), eDF, 1),
      TableInfo("f", 10, 10, Map[String, Long]("s"->13, "y"->10), fDF, 1)
    )

    Arguments(attributes, keys, joinConditions, relations, info)
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

    val aDF = spark.sparkContext.parallelize(
      XData(1) ::
        XData(2) ::
        XData(3) ::
        XData(4) ::
        XData(5) ::
        XData(6) :: Nil, 2).toDF()
    aDF.createOrReplaceTempView("a")

    val bDF = spark.sparkContext.parallelize(
      XyData(1, 1) ::
        XyData(1, 2) ::
        XyData(2, 1) ::
        XyData(2, 2) ::
        XyData(3, 1) ::
        XyData(3, 2) :: Nil, 2).toDF()
    bDF.createOrReplaceTempView("b")

    val cDF = spark.sparkContext.parallelize(
      YzData(1, 1) ::
        YzData(1, 2) ::
        YzData(2, 1) ::
        YzData(2, 2) ::
        YzData(3, 1) ::
        YzData(3, 2) :: Nil, 2).toDF()
    cDF.createOrReplaceTempView("c")

    val dDF = spark.sparkContext.parallelize(
      ZData(1) ::
        ZData(2) ::
        ZData(3) ::
        ZData(4) ::
        ZData(5) ::
        ZData(6) :: Nil, 2).toDF()
    dDF.createOrReplaceTempView("d")

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("d", 10, 10, Map[String, Long]("z"->9), dDF, 1)
    )

    Arguments(attributes, keys, joinConditions, relations, info)
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

    val aDF = spark.sparkContext.parallelize(
      XzData(1, 1) ::
        XzData(1, 2) ::
        XzData(2, 1) ::
        XzData(2, 2) ::
        XzData(3, 1) ::
        XzData(3, 2) :: Nil, 2).toDF()
    aDF.createOrReplaceTempView("a")

    val bDF = spark.sparkContext.parallelize(
      XyData(1, 1) ::
        XyData(1, 2) ::
        XyData(2, 1) ::
        XyData(2, 2) ::
        XyData(3, 1) ::
        XyData(3, 2) :: Nil, 2).toDF()
    bDF.createOrReplaceTempView("b")

    val cDF = spark.sparkContext.parallelize(
      YzData(1, 1) ::
        YzData(1, 2) ::
        YzData(2, 1) ::
        YzData(2, 2) ::
        YzData(3, 1) ::
        YzData(3, 2) :: Nil, 2).toDF()
    cDF.createOrReplaceTempView("c")

    val dDF = spark.sparkContext.parallelize(
      ZpData(1, 1) ::
        ZpData(1, 2) ::
        ZpData(2, 1) ::
        ZpData(2, 2) ::
        ZpData(3, 1) ::
        ZpData(3, 2) :: Nil, 2).toDF()
    dDF.createOrReplaceTempView("d")

    val eDF = spark.sparkContext.parallelize(
      PqData(1, 1) ::
        PqData(1, 2) ::
        PqData(2, 1) ::
        PqData(2, 2) ::
        PqData(3, 1) ::
        PqData(3, 2) :: Nil, 2).toDF()
    eDF.createOrReplaceTempView("e")

    val fDF = spark.sparkContext.parallelize(
      QsData(1, 1) ::
        QsData(1, 2) ::
        QsData(2, 1) ::
        QsData(2, 2) ::
        QsData(3, 1) ::
        QsData(3, 2) :: Nil, 2).toDF()
    fDF.createOrReplaceTempView("f")

    val gDF = spark.sparkContext.parallelize(
      SData(1) ::
        SData(2) ::
        SData(3) ::
        SData(4) ::
        SData(5) ::
        SData(6) :: Nil, 2).toDF()
    gDF.createOrReplaceTempView("g")

    val hDF = spark.sparkContext.parallelize(
      YrData(1, 1) ::
        YrData(1, 2) ::
        YrData(2, 1) ::
        YrData(2, 2) ::
        YrData(3, 1) ::
        YrData(3, 2) :: Nil, 2).toDF()
    hDF.createOrReplaceTempView("h")

    val iDF = spark.sparkContext.parallelize(
      RwData(1, 1) ::
        RwData(1, 2) ::
        RwData(2, 1) ::
        RwData(2, 2) ::
        RwData(3, 1) ::
        RwData(3, 2) :: Nil, 2).toDF()
    iDF.createOrReplaceTempView("i")

    val jDF = spark.sparkContext.parallelize(
      WData(1) ::
        WData(2) ::
        WData(3) ::
        WData(4) ::
        WData(5) ::
        WData(6) :: Nil, 2).toDF()
    jDF.createOrReplaceTempView("j")

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("d", 10, 10, Map[String, Long]("z"->5, "p"->9), dDF, 1),
      TableInfo("e", 10, 10, Map[String, Long]("p"->6, "q"->7), eDF, 1),
      TableInfo("f", 10, 10, Map[String, Long]("q"->8, "s"->10), fDF, 1),
      TableInfo("g", 10, 10, Map[String, Long]("s"->5), gDF, 1),
      TableInfo("h", 10, 10, Map[String, Long]("y"->6, "r"->7), hDF, 1),
      TableInfo("i", 10, 10, Map[String, Long]("r"->8, "w"->10), iDF, 1),
      TableInfo("j", 10, 10, Map[String, Long]("w"->8), jDF, 1)
    )
    Arguments(attributes, keys, joinConditions, relations, info)
  }
  def readFrom(fileName: String): Seq[Row] = {
    val classLoader = getClass().getClassLoader
    val rawData = IOUtils.lineIterator(classLoader.getResourceAsStream(fileName), "UTF-8")

    val s: mutable.ArrayBuffer[Row] = mutable.ArrayBuffer[Row]()
    while (rawData.hasNext) {
      val line = rawData.nextLine()
      s += Row(line.substring(1, line.length - 1).split(",").map (n => n.toInt):_*)
    }
    s
  }
  def expectedTriangle(): Seq[Row] = readFrom("triangle")
  def expectedClique(): Seq[Row] = readFrom("clique")
  def expectedLine(): Seq[Row] = readFrom("line")
  def expectedArbitrary(): Seq[Row] = readFrom("arbitrary")
}
object ArgumentsSet {
  case class XzData(x: Int, z: Int)
  case class XyData(x: Int, y: Int)
  case class YzData(y: Int, z: Int)
  case class SzData(s: Int, z: Int)
  case class SxData(s: Int, x: Int)
  case class SyData(s: Int, y: Int)
  case class XData(x: Int)
  case class ZData(z: Int)
  case class ZpData(z: Int, p: Int)
  case class PqData(p: Int, q: Int)
  case class QsData(q: Int, s: Int)
  case class SData(s: Int)
  case class YrData(y: Int, r: Int)
  case class RwData(r: Int, w: Int)
  case class WData(w: Int)
}