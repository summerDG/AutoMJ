package org.pasalab.automj

import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal, Multiply, Murmur3Hash, Pmod, Unevaluable}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

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

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Seq[Expression] = {
    //    Pmod(new Murmur3Hash(expressions), Literal(4)) :: Nil
    val (hashNumbers, dimIds) = computeExpressionToIds()
    var i = 0
    val dims = new Array[Int](shares.length)
    while (i < shares.length) {
      if (i == 0) dims(i) = numPartitions / shares(i)
      else dims(i) = dims(i - 1) / shares(i)
      i += 1
    }

    val nonStar = hashNumbers.zip(dimIds).map {
      case (hash, index) =>
        Multiply(hash, Literal(dims(index)))
    }.fold(Literal(0))((x, y) => Add(x, y))
    if (shares.length - hashNumbers.length > 0) {
      val usedDims = dimIds.toSet
      dims.zipWithIndex.filterNot(t => usedDims.contains(t._2))
        .flatMap{
          case (range, index) =>
            (0 until shares(index)).map( i => Add(Literal(i * range), nonStar))
        }
    } else {
      nonStar :: Nil
    }
  }
}
case class HcDistribution(clustering: Seq[Expression],
                                 numPartitions: Int,
                                 dimensionToExpressions: Seq[Seq[Expression]],
                                 shares: Seq[Int])
