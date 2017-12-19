package org.apache.spark.sql.execution.exchange

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, Expression, SortOrder, SortPrefix, UnsafeProjection}
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
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  override def nodeName: String = "ShareExchange"

  var postPartitioning: Partitioning = null
  override def outputPartitioning: Partitioning =
    if (postPartitioning != null) postPartitioning else UnknownPartitioning(partitioning.numPartitions)
  private def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
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

  private def preparePostShuffleRDD(
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

  private var cachedShuffleRDD: ShuffledRowRDD = null
  override protected def doExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = postShuffleRDD
    }
    cachedShuffleRDD
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
