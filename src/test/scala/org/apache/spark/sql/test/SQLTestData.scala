/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.test

import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.pasalab.automj.{Arguments, TableInfo}

import scala.collection.mutable

/**
 * A collection of sample data used in SQL tests.
 */
private[sql] trait SQLTestData { self =>
  protected def spark: SparkSession

  protected def sqlConf: SQLConf = spark.sessionState.conf

  // Helper object to import SQL implicits without a concrete SQLContext
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import internalImplicits._
  import SQLTestData._

  // Note: all test data should be lazy because the SQLContext is not set up yet.

  protected lazy val emptyTestData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Seq.empty[Int].map(i => TestData(i, i.toString))).toDF()
    df.createOrReplaceTempView("emptyTestData")
    df
  }

  protected lazy val testData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
    df.createOrReplaceTempView("testData")
    df
  }

  protected lazy val testData2: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestData2(1, 1) ::
        TestData2(1, 2) ::
        TestData2(2, 1) ::
        TestData2(2, 2) ::
        TestData2(3, 1) ::
        TestData2(3, 2) :: Nil, 2).toDF()
    df.createOrReplaceTempView("testData2")
    df
  }

  protected lazy val testData3: DataFrame = {
    val df = spark.sparkContext.parallelize(
      TestData3(1, None) ::
        TestData3(2, Some(2)) :: Nil).toDF()
    df.createOrReplaceTempView("testData3")
    df
  }

  protected lazy val negativeData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(-i, (-i).toString))).toDF()
    df.createOrReplaceTempView("negativeData")
    df
  }

  protected lazy val largeAndSmallInts: DataFrame = {
    val df = spark.sparkContext.parallelize(
      LargeAndSmallInts(2147483644, 1) ::
        LargeAndSmallInts(1, 2) ::
        LargeAndSmallInts(2147483645, 1) ::
        LargeAndSmallInts(2, 2) ::
        LargeAndSmallInts(2147483646, 1) ::
        LargeAndSmallInts(3, 2) :: Nil).toDF()
    df.createOrReplaceTempView("largeAndSmallInts")
    df
  }

  protected lazy val decimalData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      DecimalData(1, 1) ::
        DecimalData(1, 2) ::
        DecimalData(2, 1) ::
        DecimalData(2, 2) ::
        DecimalData(3, 1) ::
        DecimalData(3, 2) :: Nil).toDF()
    df.createOrReplaceTempView("decimalData")
    df
  }

  protected lazy val binaryData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      BinaryData("12".getBytes(StandardCharsets.UTF_8), 1) ::
        BinaryData("22".getBytes(StandardCharsets.UTF_8), 5) ::
        BinaryData("122".getBytes(StandardCharsets.UTF_8), 3) ::
        BinaryData("121".getBytes(StandardCharsets.UTF_8), 2) ::
        BinaryData("123".getBytes(StandardCharsets.UTF_8), 4) :: Nil).toDF()
    df.createOrReplaceTempView("binaryData")
    df
  }

  protected lazy val upperCaseData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      UpperCaseData(1, "A") ::
        UpperCaseData(2, "B") ::
        UpperCaseData(3, "C") ::
        UpperCaseData(4, "D") ::
        UpperCaseData(5, "E") ::
        UpperCaseData(6, "F") :: Nil).toDF()
    df.createOrReplaceTempView("upperCaseData")
    df
  }

  protected lazy val lowerCaseData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil).toDF()
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  protected lazy val arrayData: RDD[ArrayData] = {
    val rdd = spark.sparkContext.parallelize(
      ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3))) ::
        ArrayData(Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
    rdd.toDF().createOrReplaceTempView("arrayData")
    rdd
  }

  protected lazy val mapData: RDD[MapData] = {
    val rdd = spark.sparkContext.parallelize(
      MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
        MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
        MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
        MapData(Map(1 -> "a4", 2 -> "b4")) ::
        MapData(Map(1 -> "a5")) :: Nil)
    rdd.toDF().createOrReplaceTempView("mapData")
    rdd
  }

  protected lazy val repeatedData: RDD[StringData] = {
    val rdd = spark.sparkContext.parallelize(List.fill(2)(StringData("test")))
    rdd.toDF().createOrReplaceTempView("repeatedData")
    rdd
  }

  protected lazy val nullableRepeatedData: RDD[StringData] = {
    val rdd = spark.sparkContext.parallelize(
      List.fill(2)(StringData(null)) ++
        List.fill(2)(StringData("test")))
    rdd.toDF().createOrReplaceTempView("nullableRepeatedData")
    rdd
  }

  protected lazy val nullInts: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullInts(1) ::
        NullInts(2) ::
        NullInts(3) ::
        NullInts(null) :: Nil).toDF()
    df.createOrReplaceTempView("nullInts")
    df
  }

  protected lazy val allNulls: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullInts(null) ::
        NullInts(null) ::
        NullInts(null) ::
        NullInts(null) :: Nil).toDF()
    df.createOrReplaceTempView("allNulls")
    df
  }

  protected lazy val nullStrings: DataFrame = {
    val df = spark.sparkContext.parallelize(
      NullStrings(1, "abc") ::
        NullStrings(2, "ABC") ::
        NullStrings(3, null) :: Nil).toDF()
    df.createOrReplaceTempView("nullStrings")
    df
  }

  protected lazy val tableName: DataFrame = {
    val df = spark.sparkContext.parallelize(TableName("test") :: Nil).toDF()
    df.createOrReplaceTempView("tableName")
    df
  }

  protected lazy val unparsedStrings: RDD[String] = {
    spark.sparkContext.parallelize(
      "1, A1, true, null" ::
        "2, B2, false, null" ::
        "3, C3, true, null" ::
        "4, D4, true, 2147483644" :: Nil)
  }

  // An RDD with 4 elements and 8 partitions
  protected lazy val withEmptyParts: RDD[IntField] = {
    val rdd = spark.sparkContext.parallelize((1 to 4).map(IntField), 8)
    rdd.toDF().createOrReplaceTempView("withEmptyParts")
    rdd
  }

  protected lazy val person: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Person(0, "mike", 30) ::
        Person(1, "jim", 20) :: Nil).toDF()
    df.createOrReplaceTempView("person")
    df
  }

  protected lazy val salary: DataFrame = {
    val df = spark.sparkContext.parallelize(
      Salary(0, 2000.0) ::
        Salary(1, 1000.0) :: Nil).toDF()
    df.createOrReplaceTempView("salary")
    df
  }

  protected lazy val complexData: DataFrame = {
    val df = spark.sparkContext.parallelize(
      ComplexData(Map("1" -> 1), TestData(1, "1"), Seq(1, 1, 1), true) ::
        ComplexData(Map("2" -> 2), TestData(2, "2"), Seq(2, 2, 2), false) ::
        Nil).toDF()
    df.createOrReplaceTempView("complexData")
    df
  }

  protected lazy val courseSales: DataFrame = {
    val df = spark.sparkContext.parallelize(
      CourseSales("dotNET", 2012, 10000) ::
        CourseSales("Java", 2012, 20000) ::
        CourseSales("dotNET", 2012, 5000) ::
        CourseSales("dotNET", 2013, 48000) ::
        CourseSales("Java", 2013, 30000) :: Nil).toDF()
    df.createOrReplaceTempView("courseSales")
    df
  }

  protected lazy val triangleData: Arguments = {
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

    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      aDF.logicalPlan, bDF.logicalPlan, cDF.logicalPlan)

    val attributes: Map[String, Attribute] = Map[String, Attribute] (
      "a.x"->relations(0).output(0),
      "b.x"->relations(1).output(0),
      "b.y"->relations(1).output(1),
      "c.y"->relations(2).output(0),
      "a.z"->relations(0).output(1),
      "c.z"->relations(2).output(1))
    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("a.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (0, 2)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("c.z")))
    )

    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x"), attributes("a.z")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z"))
    )

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1)
    )

    spark.sessionState.catalog match {
      case catalog: MjSessionCatalog =>
        info.foreach(i => catalog.registerTable(i.name, i.size, i.count, i.cardinality, i.sample, i.p))
    }

    Arguments(attributes, keys, joinConditions, relations, info)
  }

  protected lazy val cliqueData: Arguments = {
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

    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      aDF.logicalPlan, bDF.logicalPlan, cDF.logicalPlan, dDF.logicalPlan, eDF.logicalPlan, fDF.logicalPlan)

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("d", 10, 10, Map[String, Long]("s"->11, "z"->9), dDF, 1),
      TableInfo("e", 10, 10, Map[String, Long]("s"->12, "x"->7), eDF, 1),
      TableInfo("f", 10, 10, Map[String, Long]("s"->13, "y"->10), fDF, 1)
    )

    spark.sessionState.catalog match {
      case catalog: MjSessionCatalog =>
        info.foreach(i => catalog.registerTable(i.name, i.size, i.count, i.cardinality, i.sample, i.p))
    }

    Arguments(attributes, keys, joinConditions, relations, info)
  }

  protected lazy val lineData: Arguments = {
    val aDF = spark.sparkContext.parallelize(
      XData(1) ::
        XData(2) ::
        XData(3) ::
        XData(4) ::
        XData(5) ::
        XData(6) :: Nil, 2).toDF()
    aDF.createOrReplaceTempView("al")

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
    dDF.createOrReplaceTempView("dl")

    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      aDF.logicalPlan, bDF.logicalPlan, cDF.logicalPlan, dDF.logicalPlan)

    val attributes: Map[String, Attribute] = Map[String, Attribute] (
      "al.x"->relations(0).output(0),
      "b.x"->relations(1).output(0),
      "b.y"->relations(1).output(1),
      "c.y"->relations(2).output(0),
      "c.z"->relations(2).output(1),
      "dl.z"->relations(3).output(0))

    val joinConditions: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = Map[(Int, Int), (Seq[Expression], Seq[Expression])](
      (0, 1)->(Seq[Expression](attributes("al.x")), Seq[Expression](attributes("b.x"))),
      (1, 2)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("c.y"))),
      (2, 3)->(Seq[Expression](attributes("c.z")), Seq[Expression](attributes("dl.z")))
    )

    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
    Seq[Expression](attributes("al.x")),
    Seq[Expression](attributes("b.x"), attributes("b.y")),
    Seq[Expression](attributes("c.y"), attributes("c.z")),
    Seq[Expression](attributes("dl.z"))
    )

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("al", 10, 10, Map[String, Long]("x"->5), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("dl", 10, 10, Map[String, Long]("z"->9), dDF, 1)
    )
    spark.sessionState.catalog match {
      case catalog: MjSessionCatalog =>
        info.foreach(i => catalog.registerTable(i.name, i.size, i.count, i.cardinality, i.sample, i.p))
    }

    Arguments(attributes, keys, joinConditions, relations, info)
  }
  protected lazy val arbitraryData: Arguments = {
    val attributes: Map[String, AttributeReference] = Map[String, AttributeReference] (
      "a.x"->AttributeReference("x", IntegerType)(exprId = ExprId(1)),
      "b.x"->AttributeReference("x", IntegerType)(exprId = ExprId(2)),
      "b.y"->AttributeReference("y", IntegerType)(exprId = ExprId(3)),
      "c.y"->AttributeReference("y", IntegerType)(exprId = ExprId(4)),
      "a.z"->AttributeReference("z", IntegerType)(exprId = ExprId(5)),
      "c.z"->AttributeReference("z", IntegerType)(exprId = ExprId(6)),
      "da.z"->AttributeReference("z", IntegerType)(exprId = ExprId(7)),
      "da.p"->AttributeReference("p", IntegerType)(exprId = ExprId(8)),
      "ea.p"->AttributeReference("p", IntegerType)(exprId = ExprId(9)),
      "ea.q"->AttributeReference("q", IntegerType)(exprId = ExprId(10)),
      "fa.q"->AttributeReference("q", IntegerType)(exprId = ExprId(11)),
      "fa.s"->AttributeReference("s", IntegerType)(exprId = ExprId(12)),
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
      (0, 3)->(Seq[Expression](attributes("a.z")), Seq[Expression](attributes("da.z"))),
      (3, 4)->(Seq[Expression](attributes("da.p")), Seq[Expression](attributes("ea.p"))),
      (4, 5)->(Seq[Expression](attributes("ea.q")), Seq[Expression](attributes("fa.q"))),
      (5, 6)->(Seq[Expression](attributes("fa.s")), Seq[Expression](attributes("g.s"))),
      (1, 7)->(Seq[Expression](attributes("b.y")), Seq[Expression](attributes("h.y"))),
      (7, 8)->(Seq[Expression](attributes("h.r")), Seq[Expression](attributes("i.r"))),
      (8, 9)->(Seq[Expression](attributes("i.w")), Seq[Expression](attributes("j.w")))
    )

    val keys:Seq[Seq[Expression]] = Seq[Seq[Expression]](
      Seq[Expression](attributes("a.x"), attributes("a.z")),
      Seq[Expression](attributes("b.x"), attributes("b.y")),
      Seq[Expression](attributes("c.y"), attributes("c.z")),
      Seq[Expression](attributes("da.z"), attributes("da.p")),
      Seq[Expression](attributes("ea.p"), attributes("ea.q")),
      Seq[Expression](attributes("fa.q"), attributes("fa.s")),
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
    dDF.createOrReplaceTempView("da")

    val eDF = spark.sparkContext.parallelize(
      PqData(1, 1) ::
        PqData(1, 2) ::
        PqData(2, 1) ::
        PqData(2, 2) ::
        PqData(3, 1) ::
        PqData(3, 2) :: Nil, 2).toDF()
    eDF.createOrReplaceTempView("ea")

    val fDF = spark.sparkContext.parallelize(
      QsData(1, 1) ::
        QsData(1, 2) ::
        QsData(2, 1) ::
        QsData(2, 2) ::
        QsData(3, 1) ::
        QsData(3, 2) :: Nil, 2).toDF()
    fDF.createOrReplaceTempView("fa")

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

    val relations: Seq[LogicalPlan] = Seq[LogicalPlan](
      aDF.logicalPlan, bDF.logicalPlan, cDF.logicalPlan, dDF.logicalPlan, eDF.logicalPlan,
      fDF.logicalPlan, gDF.logicalPlan, hDF.logicalPlan, iDF.logicalPlan, jDF.logicalPlan)

    val info: Seq[TableInfo] = Seq[TableInfo](
      TableInfo("a", 10, 10, Map[String, Long]("x"->5, "z"->9), aDF, 1),
      TableInfo("b", 10, 10, Map[String, Long]("x"->6, "y"->7), bDF, 1),
      TableInfo("c", 10, 10, Map[String, Long]("y"->8, "z"->10), cDF, 1),
      TableInfo("da", 10, 10, Map[String, Long]("z"->5, "p"->9), dDF, 1),
      TableInfo("ea", 10, 10, Map[String, Long]("p"->6, "q"->7), eDF, 1),
      TableInfo("fa", 10, 10, Map[String, Long]("q"->8, "s"->10), fDF, 1),
      TableInfo("g", 10, 10, Map[String, Long]("s"->5), gDF, 1),
      TableInfo("h", 10, 10, Map[String, Long]("y"->6, "r"->7), hDF, 1),
      TableInfo("i", 10, 10, Map[String, Long]("r"->8, "w"->10), iDF, 1),
      TableInfo("j", 10, 10, Map[String, Long]("w"->8), jDF, 1)
    )

    spark.sessionState.catalog match {
      case catalog: MjSessionCatalog =>
        info.foreach(i => catalog.registerTable(i.name, i.size, i.count, i.cardinality, i.sample, i.p))
    }
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

  /**
   * Initialize all test data such that all temp tables are properly registered.
   */
  def loadTestData(): Unit = {
    assert(spark != null, "attempted to initialize test data before SparkSession.")
    emptyTestData
    testData
    testData2
    testData3
    negativeData
    largeAndSmallInts
    decimalData
    binaryData
    upperCaseData
    lowerCaseData
    arrayData
    mapData
    repeatedData
    nullableRepeatedData
    nullInts
    allNulls
    nullStrings
    tableName
    unparsedStrings
    withEmptyParts
    person
    salary
    complexData
    courseSales

    triangleData
    cliqueData
    lineData
    arbitraryData
  }
}

/**
 * Case classes used in test data.
 */
private[sql] object SQLTestData {
  case class TestData(key: Int, value: String)
  case class TestData2(a: Int, b: Int)
  case class TestData3(a: Int, b: Option[Int])
  case class LargeAndSmallInts(a: Int, b: Int)
  case class DecimalData(a: BigDecimal, b: BigDecimal)
  case class BinaryData(a: Array[Byte], b: Int)
  case class UpperCaseData(N: Int, L: String)
  case class LowerCaseData(n: Int, l: String)
  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  case class MapData(data: scala.collection.Map[Int, String])
  case class StringData(s: String)
  case class IntField(i: Int)
  case class NullInts(a: Integer)
  case class NullStrings(n: Int, s: String)
  case class TableName(tableName: String)
  case class Person(id: Int, name: String, age: Int)
  case class Salary(personId: Int, salary: Double)
  case class ComplexData(m: Map[String, Int], s: TestData, a: Seq[Int], b: Boolean)
  case class CourseSales(course: String, year: Int, earnings: Double)

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