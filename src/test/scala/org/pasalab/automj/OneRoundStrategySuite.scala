package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.joins.ExpressionAndAttributes

/**
 * Created by wuxiaoqi on 17-12-12.
 */
class OneRoundStrategySuite extends SharedMjContext with ArgumentsSet{
  test("test refresh method") {
    val dataSource = triangleData
    dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))

    val oneRoundStrategy: OneRoundStrategy = ShareStrategy(meta)
    oneRoundStrategy.refresh(dataSource.keys, dataSource.joinConditions, dataSource.relations, 8, None)
    val closures = oneRoundStrategy.getClosures()
    val shares = oneRoundStrategy.getShares

    val attributes = dataSource.attributes
    val tables = dataSource.relations
    val expectedClosures: Seq[Seq[(ExpressionAndAttributes, Int)]] = Seq[Seq[(ExpressionAndAttributes, Int)]](
      Seq[(ExpressionAndAttributes, Int)](
        (ExpressionAndAttributes(Seq[Expression](attributes("a.x")), tables(0).output), 0),
        (ExpressionAndAttributes(Seq[Expression](attributes("b.x")), tables(1).output), 1)
      ),
      Seq[(ExpressionAndAttributes, Int)](
        (ExpressionAndAttributes(Seq[Expression](attributes("b.y")), tables(1).output), 1),
        (ExpressionAndAttributes(Seq[Expression](attributes("c.y")), tables(2).output), 2)
      ),
      Seq[(ExpressionAndAttributes, Int)](
        (ExpressionAndAttributes(Seq[Expression](attributes("a.z")), tables(0).output), 0),
        (ExpressionAndAttributes(Seq[Expression](attributes("c.z")), tables(2).output), 2)
      )
    )
    for (i <- 0 to closures.length - 1) {
      val t = closures(i)
      val e = expectedClosures(i)
      assert(t.length == e.length && t.zip(e).forall{
        case (l, r) =>
          l._1 == r._1 && l._2 == r._2
      }, s"Computed Closure No.$i: len-${t.length}, ${t.toString()}" +
        s"Expected Closure No.$i: len-${e.length}, ${e.toString()}")
      assert(shares(i) == 2, s"Shares($i)=${shares(i)}, not 2")
    }
  }
  test("test cost model") {
    val dataSource = triangleData
    dataSource.info.foreach(info => meta.registerTable(info.name, info.size, info.count, info.cardinality, info.sample, info.p))

    val oneRoundStrategy: OneRoundStrategy = ShareStrategy(meta)
    oneRoundStrategy.refresh(dataSource.keys, dataSource.joinConditions, dataSource.relations, 8, None)
    val cost: Long = oneRoundStrategy.cost()
    assert(cost == 60, s"Communication Cost is not correct, $cost != 60")
  }
}
