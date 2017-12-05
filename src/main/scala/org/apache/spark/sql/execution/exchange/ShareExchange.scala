package org.apache.spark.sql.execution.exchange

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, Expression, SortOrder, SortPrefix, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.pasalab.automj.HcPartitioning

/**
 * Created by wuxiaoqi on 17-12-4.
 */
case class ShareExchange(partitioning: HcPartitioning,
                         projExprs: Seq[Expression],
                         child: SparkPlan) extends Exchange {
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def nodeName: String = "ShareExchange"

  var postPartitioning: Partitioning = null
  override def outputPartitioning: Partitioning =
    if (postPartitioning != null) postPartitioning else UnknownPartitioning(partitioning.numPartitions)
  private[exchange] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val serializer = new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))
    ShareExchange.prepareShuffleDependency(child.execute(), child.output,
      partitioning, serializer)
  }
  private def postShuffleRDD: ShuffledRowRDD ={
    val shuffleDependency = prepareShuffleDependency()
    val mapOutputStatistics: Option[MapOutputStatistics] = if (shuffleDependency.rdd.partitions.length != 0) {
      // submitMapStage does not accept RDD with 0 partition.
      // So, we will not submit this dependency.
      Some(sqlContext.sparkContext.submitMapStage(shuffleDependency).get())
    } else None
    val partitionStartIndices = mapOutputStatistics.map {
      case stat: MapOutputStatistics =>
        (0 until stat.bytesByPartitionId.length).toArray
    }
    preparePostShuffleRDD(shuffleDependency, partitionStartIndices)
  }

  private[exchange] def preparePostShuffleRDD(
                                               shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
                                               specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      postPartitioning = UnknownPartitioning(indices.length)
    }

    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }
  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }
  def createSorter(): UnsafeExternalRowSorter = {
    val enableRadixSort = sqlContext.conf.enableRadixSort
    val sortOrder: Seq[SortOrder] = requiredOrders(partitioning.expressions)
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = createPrefixGeneratorForHC(prefixProjection, prefixExpr)

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = new UnsafeExternalRowSorter(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    sorter
  }
  def createPrefixGeneratorForHC(
                                  prefixProjection: UnsafeProjection,
                                  prefixExpr: SortPrefix): UnsafeExternalRowSorter.PrefixComputer = {

    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
      UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }
    prefixComputer
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val sortTime = longMetric("sortTime")

    postShuffleRDD.mapPartitionsInternal { iter =>
      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += sorter.getSortTimeNanos / 1000000
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }
}
object ShareExchange {
  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
                                rdd: RDD[InternalRow],
                                outputAttributes: Seq[Attribute],
                                partitioning: HcPartitioning,
                                serializer: Serializer): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = partitioning match {
      // Add by xiaoqi wu on 2016/10/8.
      case HcPartitioning(_, n, _, _) =>
        new Partitioner {
          override def numPartitions: Int = n

          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case _ => sys.error(s"Exchange not implemented for $partitioning")
      // TODO: Handle BroadcastPartitioning.
    }
    // TODO: Handle HyperCubePartitioning.
    def getPartitionKeyExtractor(): InternalRow => Seq[Int] = partitioning match {
      // Add by xiaoqi wu on 2016/10/6.
      case hc: HcPartitioning =>
        val projections = hc.partitionIdExpression.map {
          case idExpression =>
            UnsafeProjection.create(idExpression :: Nil, outputAttributes)
        }
        row => projections.map(p => p(row).getInt(0))
      case _ => sys.error(s"$partitioning is not HyperCubeShuffleExchange.")
    }
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      rdd.mapPartitionsInternal { iter =>
        val getPartitionKey = getPartitionKeyExtractor()
        iter.flatMap {
          row =>
            val ids = getPartitionKey(row)
            val copy = row.copy()
            ids.map(id => (part.getPartition(id), copy))
        }
      }
    }
    new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer)
  }
}
