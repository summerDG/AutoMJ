package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal, Multiply, Murmur3Hash, Pmod, Unevaluable}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

/**
 * Created by wuxiaoqi on 17-12-5.
 */
case class HcPartitioning(expressions: Seq[Expression],
                          numPartitions: Int,
                          dimensionToExpressions: Seq[Seq[Expression]],
                          shares: Seq[Int]) extends Expression with Unevaluable {

  type expresionsAndIds = (Seq[Expression], Seq[Int])

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = ArrayType(IntegerType)

  private def computeExpressionToIds(
                                      dimToExprs: Seq[Seq[Expression]] = dimensionToExpressions,
                                      exprs: Seq[Expression] = expressions): expresionsAndIds = {
    val vector = dimToExprs.map {
      case wholeSet: Seq[Expression] =>
        wholeSet.flatMap {
          case e =>
            if (exprs.exists(expr => expr.semanticEquals(e))) Some(e)
            else None
        }
    }
    computeHashFromVector(vector)
  }

  private def computeHashFromVector[T <: Expression](
                                                      vector: Seq[Seq[Expression]]): expresionsAndIds = {
    assert(vector.length == shares.length,
      s"length of vector != length of shares: ${vector.length}, ${shares.length}")
    val ids = collection.mutable.ArrayBuffer[Int]()
    val exprs = vector.zipWithIndex.flatMap{
      case (innerExprs, index) =>
        if (innerExprs.length > 0) {
          ids += index
          Some(Pmod(new Murmur3Hash(innerExprs), Literal(shares(index))))
        } else {
          None
        }
    }
    (exprs, ids)
  }

  def enumCombines(dims: Seq[Int], combines: mutable.ArrayBuffer[Seq[Int]], pre: Seq[Int]): Unit = {
    val level = pre.length
    if (level < dims.length) {
      for (i <- 0 to dims(level) - 1) {
        enumCombines(dims, combines, pre:+i)
      }
    } else {
      combines += pre
    }
  }

  def partitionId(coordinate: Array[Expression], factors: Array[Int]): Expression = {
    assert(coordinate.length == factors.length, s"coordinate(${coordinate.length}), factors(${factors.length})")
    var id: Expression = null
    for (i <- 0 to factors.length - 1) {
      val cur = Multiply(coordinate(i),Literal(factors(i)))
      if (id == null) {
        id = cur
      } else {
        id = Add(id, cur)
      }
    }
    id
  }
  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Seq[Expression] = {
    val (hashNumbers, dimIds) = computeExpressionToIds()
    var len = 1
    for (i <- dimIds) {
      len *= shares(i)
    }

    val factors = new Array[Int](shares.length)

    var i = shares.length - 1

    while (i >= 0) {
      factors(i) = if (i + 1 < shares.length) factors(i + 1) * shares(i + 1) else 1
      i -= 1
    }

    val combines = mutable.ArrayBuffer[Seq[Int]]()
    val nonStarDims: IndexedSeq[Int] = (0 to shares.length - 1).filter(x => !dimIds.contains(x)).map(x => shares(x))
    enumCombines(nonStarDims, combines, Seq[Int]())

    val coordinates: Array[Array[Expression]] = combines.map {
      case s: Seq[Int] =>
        assert(s.length + dimIds.length == shares.length, s"s(${s.length}), dimIds:${dimIds.length}")
        val t = new Array[Expression](shares.length)
        var a = 0
        var b = 0
//        var max = 0
        for (i <- 0 to shares.length - 1 if (a < dimIds.length || b < s.length)) {
          if (a < dimIds.length && dimIds(a) == i) {
            t(i) = hashNumbers(a)
//            max += (shares(i) - 1) * factors(i)
            a += 1
          } else {
            t(i) = Literal(s(b))
//            max += s(b) * factors(i)
            b += 1
          }
        }
//        assert(max < numPartitions, s"max: ${max}")
        t
    }.toArray

//    assert(false, s"coordinates: ${coordinates.map (_.mkString(",")).mkString("\n")}")
    coordinates.map(c => partitionId(c, factors))
  }
}
case class HcDistribution(clustering: Seq[Expression],
                                 numPartitions: Int,
                                 dimensionToExpressions: Seq[Seq[Expression]],
                                 shares: Seq[Int])
