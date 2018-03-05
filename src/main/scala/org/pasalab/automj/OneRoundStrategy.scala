package org.pasalab.automj

import org.apache.spark.{MjStatistics, SampleStat}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.automj.MjSessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.KeysAndTableId
import org.apache.spark.sql.execution.joins.ExpressionAndAttributes
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by wuxiaoqi on 17-12-3.
 */
abstract class OneRoundStrategy(val catalog: MjSessionCatalog, conf: SQLConf) extends AttributesOrder with Logging{
  protected var reorderedKeysEachTable: Seq[Seq[Expression]] = null
  protected var bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])] = null
  protected var relations: Seq[LogicalPlan] = null
  protected var otherCondition: Option[Expression] = None
  protected var numShufflePartitions: Int = 0

  protected var dimensionToExprs: Array[Seq[KeysAndTableId]] = null
  protected var shares: Seq[Int] = null
  protected var closures: Seq[Seq[(ExpressionAndAttributes, Int)]] = null
  protected var sizesInBytes: Seq[BigInt] = null


  def loadArgument(joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                   tables: Seq[LogicalPlan],
                   sizes: Seq[Long],
                   samples: Seq[SampleStat[Any]],
                   partitions: Int): Unit = {
    bothKeysEachCondition = joins
    relations = tables
    val partitionNum = partitions

    val initEdges = bothKeysEachCondition.toSeq.map {
      case ((l, r), (lk, rk)) =>
        (AttributeVertex(l, lk), AttributeVertex(r, rk))
    }
    val equivalenceClasses = Graph(initEdges).connectComponent()
    assert(equivalenceClasses.forall(_.nonEmpty),
      s"equivalenceClasses(${equivalenceClasses.length})," +
        s" ${if (equivalenceClasses.forall(_.isEmpty)) "is all empty" else "has some empty"})")
    dimensionToExprs = attrOptimization(samples, equivalenceClasses)
    assert(dimensionToExprs.forall(_.nonEmpty),
      s"dimensionToExprs(${dimensionToExprs.length})," +
        s" ${if (dimensionToExprs.forall(_.isEmpty)) "is all empty" else "has some empty"})")

    val buffer = new Array[ArrayBuffer[Expression]](relations.length)
      .map(_ => ArrayBuffer[Expression]())
    for (dim <- dimensionToExprs; keysAndId <- dim) {
      buffer(keysAndId.tableId) ++= keysAndId.keys
    }
    reorderedKeysEachTable = buffer.map(_.toSeq)

    closures = dimensionToExprs.map {
      case c =>
        c.map {
          case keysAndId =>
            (ExpressionAndAttributes(keysAndId.keys, relations(keysAndId.tableId).output),
              keysAndId.tableId)
        }
    }
    val (s, n) = computeSharesAndPartitions(relations.length, closures, sizes, partitionNum)
    sizesInBytes = sizes.zip(samples).map {
      case (c, s) =>
        val tupleSize = ByteArrayUtils.objectToBytes(s.sample.head) match {
          case Some(bytes) =>
            bytes.length
          case _ => 0
        }
        BigInt(c * tupleSize)
    }
    shares = s
    numShufflePartitions = n
  }
  def refresh(keys: Seq[Seq[Expression]],
              joins:Map[(Int, Int), (Seq[Expression], Seq[Expression])],
              tables: Seq[LogicalPlan],
              partitions: Int,
              conditions: Option[Expression] = None): Unit = {
    bothKeysEachCondition = joins
    relations = tables
    otherCondition = conditions
    val partitionNum = partitions

    val initEdges = bothKeysEachCondition.toSeq.map {
      case ((l, r), (lk, rk)) =>
        (AttributeVertex(l, lk), AttributeVertex(r, rk))
    }
    val equivalenceClasses = Graph(initEdges).connectComponent()
    assert(equivalenceClasses.forall(_.nonEmpty),
      s"equivalenceClasses(${equivalenceClasses.length})," +
        s" ${if (equivalenceClasses.forall(_.isEmpty)) "is all empty" else "has some empty"})")

    val statistics: Seq[MjStatistics] = relations.flatMap(x => catalog.getStatistics(x))
    assert(statistics.length == relations.length, s"some relation has no statistics(${statistics.length})")
//    assert(false, s"${equivalenceClasses.zipWithIndex.map {
//      case (s, num) =>
//        s"$num. ${s.map(n => s"relation: ${n.v.rId}, exprs: ${n.v.k.mkString(",")}")}"
//    }.mkString("\n")
//    }\n conditions: ${bothKeysEachCondition.map{
//      case ((l, r), (lk, rk)) =>
//        s"($l, $r)->($lk, $rk)"
//    }.mkString("\n")}")

    val exprToCid: Map[Long, Int] = equivalenceClasses.zipWithIndex.flatMap {
      case (nodes, cId) =>
        nodes.flatMap {
          case node =>
            node.v.k.map {
              case e: NamedExpression =>
                (e.exprId.id, cId)
            }
        }
    }.toMap

    dimensionToExprs =
      attrOptimization(equivalenceClasses.length, relations, statistics, exprToCid)
    assert(dimensionToExprs.forall(_.nonEmpty),
      s"dimensionToExprs(${dimensionToExprs.length})," +
        s" ${if (dimensionToExprs.forall(_.isEmpty)) "is all empty" else "has some empty"})")

    val buffer = new Array[ArrayBuffer[Expression]](relations.length)
      .map(x => ArrayBuffer[Expression]())
    for (dim <- dimensionToExprs; keysAndId <- dim) {
      buffer(keysAndId.tableId) ++= keysAndId.keys
    }
    reorderedKeysEachTable = buffer.map(_.toSeq)

    closures = dimensionToExprs.map {
      case c =>
        c.map {
          case keysAndId =>
            (ExpressionAndAttributes(keysAndId.keys, relations(keysAndId.tableId).output),
              keysAndId.tableId)
        }
    }
    val predictedSizes: Seq[Long] = statistics.map(_.rowCount.get.toLong)
    val (s, n) = computeSharesAndPartitions(relations.length, closures, predictedSizes, partitionNum)
    sizesInBytes = statistics.map(_.sizeInBytes)
    shares = s
    numShufflePartitions = n
  }

  def getClosures(): Seq[Seq[(ExpressionAndAttributes, Int)]] = closures
  def getShares: Seq[Int] = shares

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
          // 如果得出来的share正好是一个整数，那么就不用找邻居了
          if (shares(col) - shares(col).asInstanceOf[Int] == 0) {
            cases(row)(col) = shares(col).asInstanceOf[Int]
          } else {
            if ((row / part) % 2 == 0) {
              cases(row)(col) = shares(col).asInstanceOf[Int]
            } else {
              cases(row)(col) = shares(col).asInstanceOf[Int] + 1
            }
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
      logInfo(s"shares: ${novelShares.mkString("[",",","]")}, partitions: $novel")

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

  protected def optimizeCore: LogicalPlan

  def optimize(): LogicalPlan = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    optimizeCore
  }

  def hasArgument: Boolean = {
    reorderedKeysEachTable != null && bothKeysEachCondition != null &&
      relations != null && dimensionToExprs != null && closures != null
  }

  protected def costCore(): BigInt

  def cost(): BigInt = {
    assert(hasArgument, "Please invoke <refresh> firstly to set arguments.")
    costCore
  }
}
