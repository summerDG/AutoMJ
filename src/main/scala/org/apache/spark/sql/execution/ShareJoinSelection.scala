package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShareJoin}
import org.apache.spark.sql.execution.exchange.ShareExchange
import org.apache.spark.sql.execution.joins.{ExpressionAndAttributes, LeapFrogJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.pasalab.automj._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by wuxiaoqi on 17-12-4.
 */
abstract class ShareJoinSelection(meta: MetaManager, conf: SQLConf) extends Strategy
  with PredicateHelper with AttributesOrder{
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ShareJoin(keysEachRelation, bothKeysEachCondition, conditions,
    relations, numShufflePartitions, equivalenceClasses) =>
      val children: Seq[SparkPlan] = relations.map(n => planLater(n))

      val statistics: Seq[TableInfo] = relations.flatMap(r => meta.getInfo(r))
      val dimensionToExprs: Array[Seq[KeysAndTableId]] =
        attrOptimization(equivalenceClasses.length, statistics)

      val buffer = new Array[ArrayBuffer[Expression]](relations.length)
        .map(x => ArrayBuffer[Expression]())
      for (dim <- dimensionToExprs; keysAndId <- dim) {
        buffer(keysAndId.tableId) ++= keysAndId.keys
      }
      val reorderedKeysEachTable: Seq[Seq[Expression]] = buffer.map(_.toSeq)

      val closures: Seq[Seq[(ExpressionAndAttributes, Int)]] = dimensionToExprs.map {
        case c =>
          c.map {
            case keysAndId =>
              (ExpressionAndAttributes(keysAndId.keys, relations(keysAndId.tableId).output),
                keysAndId.tableId)
          }
      }
      val predictedSizes: Seq[Long] = statistics.map(_.count)
      val (shares, numPartitions) = computeSharesAndPartitions(relations.length, closures, predictedSizes, numPartitions)

      val partitionings: Seq[HcPartitioning] = reorderedKeysEachTable.map {
        case exprs =>
          HcPartitioning(exprs, numPartitions,
            dimensionToExprs.map(_.flatMap(_.keys)), shares)
      }

      val exchanges: Seq[ShareExchange] = for (i <- 0 to relations.length - 1) yield {
        ShareExchange(partitionings(i), reorderedKeysEachTable(i), children(i))
      }

      LeapFrogJoinExec(keysEachRelation, bothKeysEachCondition,
        conditions, exchanges, numShufflePartitions, closures) :: Nil
  }

  def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions
  /**
   * Generate shares used in HyperCube shuffle and update number of partitions.
   * Completed on 16/10/30. By xiaoqi wu.
   */
  private def computeSharesAndPartitions(numExchanges: Int,
                                         closures: Seq[Seq[(ExpressionAndAttributes, Int)]],
                                         predictSizes: Seq[Long],
                                         numShufflePartitions: Int) : (Array[Int], Int) = {
    assert(closures.forall(_.nonEmpty), "closures is empty")
    logInfo("enter computeSharesAndPartitions")

    val timestamp = System.currentTimeMillis()
    val tableBuffers = new Array[ArrayBuffer[Int]](numExchanges)
      .map(_ => new ArrayBuffer[Int])
    var i = 0
    while (i < closures.length) {
      var j = 0
      while (j < closures(i).length) {
        val rid = closures(i)(j)._2
        tableBuffers(rid) += i
        j += 1
      }
      i += 1
    }
    val tables = tableBuffers.map(_.toArray)

    val sizes = predictSizes

    def nw(shares: Array[Int], n: Int): Double = {
      var load: Long = 0L
      for (i <- 0 until tables.length) {
        load += tables(i).map(shares).fold(1)(_ * _) * sizes(i)
      }
      load.asInstanceOf[Double] / n.asInstanceOf[Double]
    }

    def roundByProbability(s: Double): Int = {
      val low = s.asInstanceOf[Int]
      val p = s - low
      val r = new Random
      if (r.nextDouble > 1 - p) low + 1
      else low
    }

    def roundShares(shares: Array[Double], origin: Int): (Array[Int], Int) = {
      // enumarate all cases
      val caseNums = math.pow(2, shares.length).asInstanceOf[Int]
      val cases = new Array[Array[Int]](caseNums).map(_ => new Array[Int](shares.length))
      for (col <- 0 until shares.length) {
        val part = caseNums / math.pow(2, col + 1).asInstanceOf[Int]
        for (row <- 0 until caseNums) {
          if ((row / part) % 2 == 0) {
            cases(row)(col) = shares(col).asInstanceOf[Int]
          } else {
            cases(row)(col) = shares(col).asInstanceOf[Int] + 1
          }
        }
      }
      var minError = origin
      var novelShares: Array[Int] = null
      var novel = 1
      for (row <- 1 until caseNums) {
        val tmp = cases(row).fold(1)(_ * _)
        val tmpError = ((tmp - origin).abs)
        if (tmp > 0) {
          if (novelShares == null) {
            novel = tmp
            novelShares = cases(row)
            minError = tmpError
          }
          else if (tmpError < minError
            || (tmpError == minError && nw(cases(row), tmp) < nw(novelShares, novel))) {
            novel = tmp
            novelShares = cases(row)
            minError = tmpError
          }
        }
      }
      logInfo(s"shares: ${novelShares}, partitions: $novel")

      (novelShares, novel)
    }

    def sharesAndPartitions(origin: Int) : (Array[Int], Int) = {

      assert(tables.forall(_.nonEmpty), "tables is empty")
      assert(origin > 0, "numShufflePartitions is 0")
      assert(sizes.forall(_ > 0), "sizes is empty")
      assert(closures.forall(x => x.length > 1 && x.forall(_._2 >= 0)), "closures is empty")
      val base = math.log(origin)

      def muis(sizes: Seq[Long]) : Seq[Double] = {
        assert(sizes.forall(_ > 0), "sizes error")
        sizes.map(s => math.log(s) / base)
      }

      roundShares(GenerateShares.generateLp(muis(sizes).toArray, tables.map(_.map(_ + 1)), origin,
        closures.length), origin)
    }
    val timestamp_in = System.currentTimeMillis()
    logInfo(s"[TIME]Part NO.1 of computing shares: ${(timestamp_in - timestamp) / 1000.0}s")
    sharesAndPartitions(numShufflePartitions)
  }
}
case class KeysAndTableId(keys: Seq[Expression], tableId: Int) {
  override def toString: String = {
    val t = keys.fold("")(_ + "," + _)
    s"tableId: $tableId $t;"
  }
}
