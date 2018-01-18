package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.{RDD, ZippedPartitionsRDDs}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Projection, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.CompactBuffer
import org.pasalab.automj.{AttributeVertex, HcPartitioning, Node}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by wuxiaoqi on 17-11-28.
 */
case class LeapFrogJoinExec(keysEachRelation: Seq[Seq[Expression]],
                            bothKeysEachCondition: Map[(Int, Int), (Seq[Expression], Seq[Expression])],
                            conditions: Option[Expression],
                            relations: Seq[SparkPlan],
                            numShufflePartitions: Int,
                            closures: Seq[Seq[(ExpressionAndAttributes, Int)]]) extends SparkPlan {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def nodeName: String = {
    "HyperCubeMultiJoinExec"
  }
  override def output: Seq[Attribute] = {
    assert(relations.nonEmpty)
    var o = relations(0).output
    for (i <- 1 until relations.length) o ++= relations(i).output
    o
  }

  /** The partitioning are used to determine whether it need to add shuffle exchange node. */
  override def outputPartitioning: Partitioning = PartitioningCollection(relations.map(_.outputPartitioning))
  /** The distributions are used to generate puppet partitioning for child HC echange node. */
  override def requiredChildDistribution: Seq[Distribution] =
  keysEachRelation.map {
    case exprs =>
      UnspecifiedDistribution
  }

  def outputNewPartitioning: Partitioning = PartitioningCollection(relations.map(_.outputPartitioning))

  /**
   * The Projection is used to parse value from InternalRow, the following id
   * represents the corresponding table ID. The outer Seq is sorted on the closure ID.
   */
  private def createKeyGenerator(): Seq[Seq[(Projection, Int)]] = {
    closures.map(s => s.map {
      case (exprsAndAttrs, id) =>
        (UnsafeProjection.create(exprsAndAttrs.expressions, exprsAndAttrs.output), id)
    })
  }
  private def createKey(): (InternalRow => Int) = {
    row => createKeyGenerator().flatMap(s => s).find(_._2 == 0).get._1(row).getInt(0)
  }
  private def compare(): ((InternalRow, InternalRow) => Int) = {
    val keyOrdering = newNaturalAscendingOrdering(closures
      .flatMap(s => s).find(_._2 == 0).get._1.expressions.map(_.dataType))
    (row1, row2) => keyOrdering.compare(row1, row2)
  }
  // TODO: Have to modify doExecute only for inner join
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
//    assert(false, "entering LeapFrog")
    logInfo(s"[POS]LeapFrog do execute")

    ZippedPartitionsRDDs(sparkContext, relations.head.execute(), relations.tail.map(_.execute())) {
      iterators =>
        val boundCondition: (InternalRow) => Boolean = {
          conditions.map{cond =>
            newPredicate(cond, output).eval _
          }
            .getOrElse((r: InternalRow) => true)
        }

        val keyOrderings = closures.map {
          case closure: Seq[(ExpressionAndAttributes, Int)] =>
            val keys = closure.head._1.expressions
            newNaturalAscendingOrdering(keys.map(_.dataType))
        }
        val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

        new RowIterator {
          private[this] val currentMatches: Array[RowExtractor] =
            new Array[RowExtractor](relations.length)
          private[this] val currentIndexEachRelation: Array[Int] =
            new Array[Int](currentMatches.length)
          private[this] val result: Array[InternalRow] =
            new Array[InternalRow](relations.length)

          private[this] val smjScanner = new LeapFrogJoinScanner(
            createKeyGenerator,
            keyOrderings,
            iterators.map {
              case itr =>
                RowIterator.fromScala(itr)
            }
          )
          private[this] val joinRow = new MultiJoinRow(relations.length)

          if (smjScanner.findNextInnerJoinRows()) {
            for (i <- 0 until relations.length) {
              currentMatches(i) = smjScanner.getAnswer(i)
            }
            initResult
          }

          def initResult: Unit = {
            var i = 0
            while (i < result.length) {
              result(i) = currentMatches(i).get(currentIndexEachRelation(i))
              i += 1
            }
          }

          override def advanceNext(): Boolean = {
            if (currentMatches.exists(x => x == null || x.isEmpty)) return false
            val lastRid = currentMatches.length - 1
            var curId = lastRid

            while (curId >= 0) {
              if (curId == lastRid) {
                val curIndex = currentIndexEachRelation(curId)
                if (currentMatches(curId).atEnd(curIndex)) {
                  curId -= 1
                  curId match {
                    case -1 =>
                      if (smjScanner.findNextInnerJoinRows()) {
                        curId = lastRid
                        for (i <- 0 until relations.length) {
                          currentMatches(i) = smjScanner.getAnswer(i)
                          currentIndexEachRelation(i) = 0
                        }
                        initResult
                      } else return false
                    case _ =>
                      currentIndexEachRelation(curId) += 1
                  }
                } else {
                  result(curId) = currentMatches(curId).get(curIndex)
                  joinRow(result)
                  currentIndexEachRelation(curId) += 1
                  if (boundCondition(joinRow)) {
                    numOutputRows += 1
                    return true
                  }
                }
              } else {
                val curIndex = currentIndexEachRelation(curId)
                if (currentMatches(curId).atEnd(curIndex)) {
                  curId -= 1
                  curId match {
                    case -1 =>
                      if (smjScanner.findNextInnerJoinRows()) {
                        curId = lastRid
                        for (i <- 0 until relations.length) {
                          currentMatches(i) = smjScanner.getAnswer(i)
                          currentIndexEachRelation(i) = 0
                        }
                        initResult
                      } else return false
                    case _ =>
                      currentIndexEachRelation(curId) += 1
                  }
                } else {
                  result(curId) = currentMatches(curId).get(curIndex)
                  curId += 1
                  currentIndexEachRelation(curId) = 0
                }
              }
            }
            false
          }

          override def getRow: InternalRow = {
            resultProj(joinRow)
          }
        }.toScala
    }
  }

  override def children: Seq[SparkPlan] = relations
}


class LeapFrogJoinScanner(
                           keyGenerators: Seq[Seq[(Projection, Int)]],
                           keysOrdering: Seq[Ordering[InternalRow]],
                           relationIters: Seq[RowIterator]) {

  private[this] val keyGeneratorsMap: Map[Projection, Int] = keyGenerators.flatten.toMap
  private[this] val closures: Seq[Seq[Projection]] = keyGenerators.map(s => s.map(_._1))
  assert(closures.length == keysOrdering.length)

  private[this] var joinFinished = false

  private[this] val answers: Array[RowExtractor] = new Array[RowExtractor](relationIters.length)

  private[this] val iterators = new Array[TableIterator](relationIters.length)

  private[this] val tables = new Array[MutableTupleBuffer](relationIters.length)

  private[this] val localOrderedJoinField = new Array[Array[JoinField]](relationIters.length)
  private[this] val lastJoinAttrIdx = new Array[Int](relationIters.length)

  private[this] val joinFieldMapping = new Array[Array[JoinField]](closures.length)
  //  // TODO: initial it
  //  private[this] val projectionIds = keyGeneratorsMap.zipWithIndex
  //    .map(x => (x._1._1, x._2))
  private[this] var currentDepth: Int = -1
  private[this] var currentIteratorIndex: Int = _
  val ansTBB = ArrayBuffer[Array[InternalRow]]()
  val TUPLE_BATCH_SIZE = 1000 * 10

  init

  case class JoinField(proj: Projection, order: Int, tableId: Int)

  private class TableIterator(tableIdx: Int, cols: Int) {
    private var curField = -1
    var rowOnIndex = 0
    val rowIndices = new Array[Int](cols)
    private val _ranges = new Array[IteratorRange](cols)
      .map(_ => new IteratorRange(0, tables(tableIdx).numTuples))

    def next: Unit = {
      rowIndices(curField) = ranges(curField).maxRow
    }

    def currentField_= (c : Int): Unit = {
      curField = c
    }

    def ranges: Array[IteratorRange] = _ranges
    def currentField: Int = curField

    def setRow(fld: Int, r: Int): Unit = {
      assert(fld < rowIndices.length, "field index cannot exceed number of columns.")
      rowIndices(fld) = r
    }

    def setRowOfCurrentField(r : Int): Unit = {
      rowIndices(curField) = r
      assert(r < ranges(curField).maxRow && r >= ranges(curField).minRow, s"row: $r >= maxRow," +
        s"currentField: $curField, tableIdx: $tableIdx, curDepth: $currentDepth")
    }

    def row(fld: Int): Int = {
      assert(fld < rowIndices.length, "field index cannot exceed number of columns.")
      rowIndices(fld)
    }

    def rowOfCurrentField: Int = rowIndices(curField)

    class IteratorRange(s: Int, e: Int) {
      assert((s == 0 && e == 0) || s < e, "min >= max")
      var min: Int = s
      var max: Int = e
      def minRow: Int = min
      def minRow_= (i : Int): Unit = {
        min = i
        assert(min < max, "min >= max")
      }
      def maxRow: Int = max
      def maxRow_= (i: Int): Unit = {
        max = i
        assert(max > 0 && min < max, "max <= 0 or min >= max")
      }
      def setRange(s: Int, e: Int) : Unit = {
        min = s
        max = e
        assert(max > 0 && min < max, "max <= 0 or min >= max")
      }
      def setRange(ir: IteratorRange): Unit = {
        setRange(ir.minRow, ir.maxRow)
      }
    }
  }

  def tuples: Array[RowExtractor] = answers
  def getAnswer: Array[RowExtractor] = answers

  private def ceilCompare(x: InternalRow, y: InternalRow): Int = {
    val comp = x.getLong(0) - y.getLong(0)
    comp match {
      case c if c > 0 => 1
      case c if c < 0 => -1
      case _ => 0
    }
  }
  private def initIterators: Unit = {
    var i = 0
    while (i < iterators.length) {
      iterators(i) = new TableIterator(i, localOrderedJoinField(i).length)
      i += 1
    }
  }

  private def init : Unit = {
    // init tables
    var i = 0
    if (tables.exists(_ == null)) {
      while (i < relationIters.length) {
        tables(i) = new MutableTupleBuffer(relationIters(i)).init
        answers(i) = new RowExtractor(tables(i))
        i += 1
      }
    }

    // Init JoinFields each closure.
    val keysEachRelation = new Array[CompactBuffer[JoinField]](relationIters.length)
    i = 0
    while (i < keysEachRelation.length) {
      keysEachRelation(i) = new CompactBuffer[JoinField]()
      i += 1
    }

    i = 0
    for (ps <- closures) {
      val joinFieldMappingBuffer = new CompactBuffer[JoinField]()
      var o = 0
      for (p <- ps) {
        val id = keyGeneratorsMap(p)
        o = keysEachRelation(id).length
        val jf = new JoinField(p, o, id)
        keysEachRelation(id) +=jf
        joinFieldMappingBuffer += jf
      }
      joinFieldMapping(i) = joinFieldMappingBuffer.toArray
      i += 1
    }
    i = 0
    while (i < keysEachRelation.length) {
      val jfs = keysEachRelation(i)
      lastJoinAttrIdx(i) = jfs.length - 1
      localOrderedJoinField(i) = jfs.toArray
      i += 1
    }
  }

  /**
   * init/restart leap-frog join.
   */
  private def leapfrogInit(): Unit = {
    for (jf <- joinFieldMapping(currentDepth)) {
      val it = iterators(jf.tableId)
      jf.order match {
        case 0 =>
          it.ranges(jf.order).setRange(0, tables(jf.tableId).numTuples)
          it.currentField = jf.order
          it.setRowOfCurrentField(0)
          it.rowOnIndex = 0
        case _ =>
          val lastJf = localOrderedJoinField(jf.tableId)(jf.order - 1).order
          it.ranges(jf.order).setRange(it.ranges(lastJf))
          it.currentField = jf.order
          it.setRowOfCurrentField(it.ranges(jf.order).minRow)
      }
    }
    joinFieldMapping(currentDepth).sortBy{
      case jf: JoinField =>
        jf.proj(tables(jf.tableId).get(iterators(jf.tableId).rowOfCurrentField).get)
    }(keysOrdering(currentDepth))
    currentIteratorIndex = joinFieldMapping(currentDepth).size - 1
  }
  /**
   * Find next InnerJoin rows, if they exist, return true, else false.
   */
  final def findNextInnerJoinRows(): Boolean = {
    var hasNext = true
    if (currentDepth == -1) {
      // Following shortcut code handles the case that one of input tables is empty.
      if (tables.exists(x => x == null || x.isEmpty)) {
        joinFinished = true
        hasNext = false
      }
      initIterators
    }

    var i = 0
    while (i < answers.length) {
      answers(i).clear
      i += 1
    }

    if (!joinFinished) leapfrogJoin
    hasNext = !answers.exists(_.isEmpty)

    hasNext
  }


  private def restoreRange: Unit = {
    iterators(joinFieldMapping(currentDepth)(currentIteratorIndex).tableId).next

    for ( jf <- joinFieldMapping(currentDepth)) {
      val localOrder = jf.order
      val it = iterators(jf.tableId)
      if(localOrder != 0) {
        val lastJf = localOrderedJoinField(jf.tableId)(localOrder - 1).order
        it.ranges(jf.order).setRange(it.ranges(lastJf))
      } else it.ranges(jf.order).maxRow = tables(jf.tableId).numTuples
    }
  }
  private def joinUp: Unit = {
    currentDepth -= 1
    joinFieldMapping(currentDepth).foreach(jf => iterators(jf.tableId).currentField = jf.order)
    currentIteratorIndex = 0

    iterators(joinFieldMapping(currentDepth)(currentIteratorIndex).tableId).next

    for ( jf <- joinFieldMapping(currentDepth)) {
      val localOrder = jf.order
      val it = iterators(jf.tableId)
      //      println(s"order: $localOrder, depth: $currentDepth")
      if(localOrder == 0) {
        it.ranges(jf.order).setRange(0, tables(jf.tableId).numTuples)
      } else {
        val lastJf = localOrderedJoinField(jf.tableId)(localOrder - 1).order
        it.ranges(jf.order).setRange(it.ranges(lastJf))
      }
    }
  }

  private def joinOpen: Unit = {
    for (jf <- joinFieldMapping(currentDepth)) {
      refineRange(jf)
    }
    currentDepth += 1
    for (jf <- joinFieldMapping(currentDepth)) {
      val table = iterators(jf.tableId)
      table.currentField = jf.order
      table.setRowOfCurrentField(table.ranges(jf.order).minRow)
    }
    leapfrogInit()
  }

  // TODO: Add specific proceed for the first JoinField.
  private def leapfrogSeek(jf: JoinField,
                           keyOrdering: Ordering[InternalRow],
                           target: InternalRow): Boolean = {
    var startRow = iterators(jf.tableId).row(jf.order)
    var endRow = iterators(jf.tableId).ranges(jf.order).maxRow - 1


    assert(startRow <= endRow, "startRow must be no less than endRow")
    var cursor = cellPointer(jf.tableId, jf.proj, endRow)

    // Refine the end of the first JoinField whose value is larger than target.

    var hasNext = true
    while (jf.order == 0 && keyOrdering.compare(cursor, target) < 0 && hasNext) {
      endRow += 1
      val table = tables(jf.tableId)
      table.get(endRow) match {
        case s: Some[InternalRow] =>
          endRow = table.numTuples - 1
          cursor = cellPointer(jf.tableId, jf.proj, endRow)
        case _ =>
          hasNext = false
          endRow = table.numTuples - 1
      }
    }

    iterators(jf.tableId).ranges(jf.order).maxRow = endRow + 1


    if (!hasNext || keyOrdering.compare(cursor, target) < 0) {
      true
    }
    else {
      var isBreak = false
      // why need this operation?
      cursor = cellPointer(jf.tableId, jf.proj, startRow)
      if (keyOrdering.compare(cursor, target) >=0) {
        cursor = cellPointer(jf.tableId, jf.proj, startRow)
        iterators(jf.tableId).setRow(jf.order, startRow)
        isBreak = true
      }

      /* binary search: find the first row whose value is not less than target */
      while (!isBreak) {
        val row = (endRow + startRow) / 2
        cursor = cellPointer(jf.tableId, jf.proj, row)
        keyOrdering.compare(cursor, target)  match {
          case c if c >= 0 => endRow = row
          case _ => startRow = row
        }
        if (startRow == endRow - 1) {
          //          assert(false, "binary search in seek")
          cursor = cellPointer(jf.tableId, jf.proj, endRow)
          iterators(jf.tableId).setRow(jf.order, endRow)
          isBreak = true
        }
      }
      false
    }
  }

  private def isInPrefix(proj: Projection): Boolean = {
    var i = currentDepth - 1
    var contain = false
    while (i >= 0 && !contain) {
      contain = closures(i).contains(proj)
      i -= 1
    }
    contain
  }


  private def cellPointer(tableId: Int, proj: Projection, row: Int) : InternalRow = {
    assert(row >= 0 && row <= tables(tableId).numTuples)
    val i = tables(tableId).get(row).get
    proj(i)
  }

  private def refineRange(jf: JoinField): Unit = {
    val keyOrdering = keysOrdering(currentDepth)
    val it = iterators(jf.tableId)
    val originStart = it.row(jf.order)
    var startRow = originStart
    var endRow = it.ranges(jf.order).maxRow - 1
    //    println(s"endRow: $endRow, order: ${jf.order}")
    iterators(jf.tableId).ranges(jf.order).minRow = startRow

    assert(startRow <= endRow, s"startRow > endRow. (startRow=$startRow, endRow=$endRow)")
    val startCursor = cellPointer(jf.tableId, jf.proj, originStart).copy()
    var cursor = cellPointer(jf.tableId, jf.proj, endRow)

    // For the first JoinField each table maye have not the correct end,
    // so it needs a specific process.
    var hasNext = true
    while (jf.order == 0 && keyOrdering.compare(startCursor, cursor) == 0 && hasNext) {
      endRow += 1
      val table = tables(jf.tableId)
      table.get(endRow) match {
        case s: Some[InternalRow] =>
          endRow = table.numTuples - 1
          cursor = cellPointer(jf.tableId, jf.proj, endRow)
        case _ =>
          hasNext = false
          endRow = table.numTuples - 1
      }
    }
    it.ranges(jf.order).maxRow = endRow + 1
    if (hasNext && keyOrdering.compare(startCursor, cursor) != 0) {
      startRow += 1
      cursor = cellPointer(jf.tableId, jf.proj, startRow)
      if (keyOrdering.compare(startCursor, cursor) < 0) {
        iterators(jf.tableId).ranges(jf.order).maxRow = startRow
      } else {
        var r = startRow
        var step = 1
        var isBreak = false
        while(!isBreak) {
          //          println(s"first part")
          val comp = keyOrdering.compare(startCursor, cursor)
          assert(comp <= 0, s"startCursor must point to an element that is not " +
            s"greater than element pointed by current cursor.\n order: ${jf.order}" +
            s"start id: ${startRow - 1} value: ${startCursor.toString}\n" +
            s"end id: ${r} value: ${cursor.toString}")
          if (comp <  0) {
            endRow = r
            isBreak = true
          } else if (comp == 0) {
            startRow = r
            r = startRow + step
            cursor = cellPointer(jf.tableId, jf.proj, r)
            step = step * 2
            if (r + step > endRow) isBreak = true
          }
        }

        isBreak = false
        while(!isBreak) {
          r = (startRow + endRow) / 2
          cursor = cellPointer(jf.tableId, jf.proj, r)
          val comp = keyOrdering.compare(startCursor, cursor)
          if (comp == 0) {
            startRow = r
          } else if (comp < 0) {
            endRow = r
          }

          if (endRow == startRow + 1) {
            iterators(jf.tableId).ranges(jf.order).maxRow = endRow
            isBreak = true
          }
        }
      }
    }
  }

  private def leapfrogSearch: Boolean = {
    var atEnd = false

    val keyOrdering = keysOrdering(currentDepth)

    val columnToProceed = joinFieldMapping(currentDepth)(currentIteratorIndex)

    val maxRow = iterators(columnToProceed.tableId).ranges(columnToProceed.order).maxRow
    val rowOfMaxkey = iterators(columnToProceed.tableId).rowOfCurrentField

    if (rowOfMaxkey == maxRow) {
      true
    }
    else {
      var maxKey = cellPointer(columnToProceed.tableId, columnToProceed.proj, rowOfMaxkey)
      assert(rowOfMaxkey <= maxRow, s"current row: $rowOfMaxkey, maxRow: $maxRow")

      nextIterator

      var isBreak = false

      while(!isBreak) {
        val fieldWithLeastKey = joinFieldMapping(currentDepth)(currentIteratorIndex)
        val rowOfMaxkey = iterators(fieldWithLeastKey.tableId).rowOfCurrentField
        val leastKey = cellPointer(fieldWithLeastKey.tableId, fieldWithLeastKey.proj, rowOfMaxkey)
        if (keyOrdering.compare(leastKey, maxKey) == 0) {
          isBreak = true
        }
        else {
          atEnd = leapfrogSeek(fieldWithLeastKey, keyOrdering, maxKey)
          if (atEnd) {
            isBreak = true
          }
          else {
            val rowOfMaxkey = iterators(fieldWithLeastKey.tableId).rowOfCurrentField
            maxKey = cellPointer(fieldWithLeastKey.tableId, fieldWithLeastKey.proj, rowOfMaxkey)
            nextIterator
          }
        }
      }

      for (jf <- joinFieldMapping(currentDepth)) {
        val maxRowJf = iterators(jf.tableId).ranges(jf.order).maxRow
        val minRowJf = iterators(jf.tableId).ranges(jf.order).minRow
        val curRow = iterators(jf.tableId).row(jf.order)
        assert(curRow >= minRowJf, s"curRow: $curRow, minRow: $minRowJf")
        assert(curRow < maxRowJf, s"jf: ${jf.tableId}, curRow: $curRow, maxRow: $maxRowJf")
      }
      atEnd
    }
  }

  private def leapfrogJoin: Unit = {

    if (currentDepth == -1) {
      currentDepth = 0
      leapfrogInit()
    }

    while (answers.forall(_.isEmpty)) {

      val atEnd = leapfrogSearch
      atEnd match {
        case true if currentDepth == 0 =>
          joinFinished = true
          return
        case true =>
          joinUp
        case false if currentDepth == closures.length - 1 =>
          // It is not need to combine buffers, cuz join exec will filter the outputs.
          for (jf <- joinFieldMapping(currentDepth)) {
            refineRange(jf)
          }
          for (i <- 0 until relationIters.length) {
            exhaustOutput(i)
          }

          iterators(joinFieldMapping(currentDepth)(currentIteratorIndex).tableId).next

          for ( jf <- joinFieldMapping(currentDepth)) {
            val localOrder = jf.order
            val it = iterators(jf.tableId)
            if(localOrder != 0) {
              val lastJf = localOrderedJoinField(jf.tableId)(localOrder - 1).order
              it.ranges(jf.order).setRange(it.ranges(lastJf))
            } else it.ranges(jf.order).maxRow = tables(jf.tableId).numTuples
          }
          return

        case _ => joinOpen
      }
    }
  }

  private def nextIterator: Unit = {
    currentIteratorIndex = (currentIteratorIndex + 1) % joinFieldMapping(currentDepth).length
  }
  private def outputTable: String = {
    tables.map(_.buf).zip(localOrderedJoinField)
      .map {
        case(rows, jfs) =>
          rows.map {
            case row =>
              val comp = keysOrdering(0).compare(jfs(0).proj(row), jfs(1).proj(row))
              s"${jfs(0).proj(row).getLong(0)}, " +
                s"${jfs(1).proj(row).getLong(0)} + $comp"
          }.fold("")(_ + "\n" + _) }
      .fold("")(_ + "-----------------------------" + _)
  }

  private def writeRecords(
                            tableIdx: Int,
                            record: Array[InternalRow]): ArrayBuffer[Array[InternalRow]] = {
    if (tableIdx == relationIters.length) return ArrayBuffer(record)
    val buffer = ArrayBuffer[Array[InternalRow]]()
    val order = iterators(tableIdx).ranges.length - 1
    val currentRow = iterators(tableIdx).ranges(order).minRow
    val maxRow = iterators(tableIdx).ranges(order).maxRow
    val jf = localOrderedJoinField(tableIdx)(order)
    assert(maxRow < currentRow + 10, s"maxRow: $maxRow, current: $currentRow;" +
      s"${jf.proj(tables(tableIdx).get(currentRow).get).getLong(0)}, " +
      s"${jf.proj(tables(tableIdx).get(maxRow - 1).get).getLong(0)}\ntable0\n")
    for (i <- currentRow until maxRow) {
      buffer ++= writeRecords(tableIdx + 1, record :+ tables(tableIdx).get(i).get)
    }
    buffer
  }
  private def addToAns(): Unit = {
    val t = new Array[InternalRow](relationIters.length)
    for (i <- 0 until relationIters.length) {
      t(i) = tables(i).get(iterators(i).rowOfCurrentField).get
    }
    ansTBB += t
  }
  private def exhaustOutput(tableIdx: Int): Unit = {
    val order = iterators(tableIdx).ranges.length - 1
    val currentRow = iterators(tableIdx).ranges(order).minRow
    val maxRow = iterators(tableIdx).ranges(order).maxRow
    answers(tableIdx).setStart(currentRow)
    answers(tableIdx).setEnd(maxRow)
  }
}

class RowExtractor(table: MutableTupleBuffer) {
  private[this] var start: Int = -1
  private[this] var end: Int = -1
  def setStart(s: Int): Unit = {
    start = s
  }
  def setEnd(e: Int): Unit = {
    end = e
  }
  def get(i: Int): InternalRow = {
    assert(start + i < end, "out of bound")
    table.get(start + i).get
  }
  def clear: Unit = {
    start = -1
    end = -1
  }
  def isEmpty: Boolean = {
    start == end
  }
  def atEnd(i: Int): Boolean = {
    start + i >= end
  }
}
private class MutableTupleBuffer(itr: RowIterator) {
  val buf: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()
  val RESERVED = 100000
  var isFull = false

  def isEmpty: Boolean = {
    !get(0).isDefined
  }

  def ahead: Boolean = {
    val hasNext = itr.advanceNext()
    //    println(s"hasNext: $hasNext")
    if (hasNext) {
      buf += itr.getRow.copy()
      //      assert(buf.size < 10000, "buffer is out of memory")
    } else isFull = true
    hasNext
  }

  def init: MutableTupleBuffer = {
    var i = 0
    while (!isFull && i < RESERVED && ahead) {
      i += 1
    }
    //    if (isFull) {
    //      assert(false, s"init is full, and tuples: $numTuples")
    //    }
    this
  }

  def get(i: Int) : Option[InternalRow] = {
    //    println(s"buf size: ${buf.size}")
    if (i < buf.size) Some(buf(i))
    else {
      val r = i - buf.size + 1
      if (grow(r)) Some(buf(i))
      else None
    }
  }

  private def grow(l: Int): Boolean = {
    var i = l + RESERVED
    //    println(s"index: $i")
    while (!isFull && i > 0 && ahead) {
      i -= 1
    }
    //    println(s"table size: ${buf.length}, $i")
    i <= RESERVED
  }

  // Predict number of tuples
  def numTuples: Int = buf.length
}

class MultiJoinRow(num: Int) extends InternalRow {
  private[this] val rows: Array[InternalRow] = new Array[InternalRow](num)

  def this(arr: Array[InternalRow]) = {
    this(arr.length)
    for (i <- 0 until num) rows(i) = arr(i)
  }
  def apply(arr: Array[InternalRow]): MultiJoinRow = {
    for (i <- 0 until num) rows(i) = arr(i)
    this
  }

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    assert(fieldTypes.length == numFields)
    val types = new Array[Seq[DataType]](rows.length)

    var i = 0
    while (i < types.length) {
      val start = if (i == 0) 0 else rows(i - 1).numFields
      types(i) = fieldTypes.slice(start, start + rows(i).numFields)
      i += 1
    }
    rows.zip(types).map{
      case (row: InternalRow, tp: Seq[DataType]) => row.toSeq(tp)
    }.reduce(_ ++ _)
  }

  override def numFields: Int = rows.map(_.numFields).sum

  override def get(i: Int, dataType: DataType): AnyRef = {
    val (id, base) = fixRange(i)
    rows(id).get(i - base, dataType)
  }

  /** Returns true if there are any NULL values in this row. */
  override def anyNull: Boolean = rows.exists(_.anyNull)

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  override def copy(): InternalRow = {
    val copys = rows.map(_.copy())
    new MultiJoinRow(copys)
  }

  def fixRange(ordinal: Int): (Int, Int) = {
    var sum = 0
    var i = 0
    var isBreak = false
    while (i < rows.length && !isBreak) {
      sum += rows(i).numFields
      if (ordinal < sum) isBreak = true
      else i += 1
    }
    (i, sum - rows(i).numFields)
  }
  override def getUTF8String(i: Int): UTF8String = {
    val (id, base) = fixRange(i)
    rows(id).getUTF8String(i - base)
  }



  override def getBinary(i: Int): Array[Byte] = {
    val (id, base) = fixRange(i)
    rows(id).getBinary(i - base)
  }

  override def getDouble(i: Int): Double = {
    val (id, base) = fixRange(i)
    rows(id).getDouble(i - base)
  }

  override def getArray(i: Int): ArrayData = {
    val (id, base) = fixRange(i)
    rows(id).getArray(i - base)
  }

  override def getInterval(i: Int): CalendarInterval = {
    val (id, base) = fixRange(i)
    rows(id).getInterval(i - base)
  }

  override def getFloat(i: Int): Float = {
    val (id, base) = fixRange(i)
    rows(id).getFloat(i - base)
  }

  override def getLong(i: Int): Long = {
    val (id, base) = fixRange(i)
    rows(id).getLong(i - base)
  }

  override def getMap(i: Int): MapData = {
    val (id, base) = fixRange(i)
    rows(id).getMap(i - base)
  }

  override def getByte(i: Int): Byte = {
    val (id, base) = fixRange(i)
    rows(id).getByte(i - base)
  }

  override def getDecimal(i: Int, precision: Int, scale: Int): Decimal = {
    val (id, base) = fixRange(i)
    rows(id).getDecimal(i - base, precision, scale)
  }

  override def getBoolean(i: Int): Boolean = {
    val (id, base) = fixRange(i)
    rows(id).getBoolean(i - base)
  }

  override def getShort(i: Int): Short = {
    val (id, base) = fixRange(i)
    rows(id).getShort(i - base)
  }

  override def getStruct(i: Int, numFields: Int): InternalRow = {
    val (id, base) = fixRange(i)
    rows(id).getStruct(i - base, numFields)
  }

  override def getInt(i: Int): Int = {
    val (id, base) = fixRange(i)
    rows(id).getInt(i - base)
  }

  override def isNullAt(i: Int): Boolean = {
    val (id, base) = fixRange(i)
    rows(id).isNullAt(i - base)
  }

  override def toString: String = {
    // Make sure toString never throws NullPointerException.
    def mayString(x: InternalRow): Option[String] = {
      if (x == null) None else Some(x.toString)
    }
    if (rows.forall(_ eq null)) {
      "[ empty row ]"
    } else {
      val flat = rows.flatMap(mayString)
      s"${if (flat.length == 1) flat(0).toString else flat.reduce(_ + "+" + _)}"
    }
  }

  override def setNullAt(i: Int): Unit = ???

  override def update(i: Int, value: Any): Unit = ???
}

case class ExpressionAndAttributes(expressions: Seq[Expression], output: Seq[Attribute]) {
  def ==(o: ExpressionAndAttributes) : Boolean = {
    if (expressions.length == o.expressions.length && output.length == o.output.length) {
      expressions.zip(o.expressions).forall{
        case (l, r) =>
          l.semanticEquals(r)
      } &&
      output.zip(o.output).forall {
        case (l, r) =>
          l.semanticEquals(r)
      }
    } else false
  }

  override def toString: String = {
    s"expressions: ${expressions.foldLeft("")(_ + _.toString())}; output: ${output.foldLeft("")(_ + _.toString())}"
  }
}
