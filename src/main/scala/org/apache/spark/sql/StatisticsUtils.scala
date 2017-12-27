package org.apache.spark.sql

import java.util.UUID

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.rdd.{PartitionwiseSampledRDDPartition, RDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.random.{BernoulliSampler, RandomSampler}
import org.apache.spark.{MjStatistics, Partition}
import org.pasalab.automj.PartitionInfo

import scala.collection.mutable
import scala.util.Random

/**
 * Created by wuxiaoqi on 17-12-22.
 */
object StatisticsUtils {
  val relativeSD: Double = 0.05
  val p0 = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
  val seed: Long = new Random().nextLong
  // attribute信息中增加一列, 列名为TableName, max为其表名
  val tableName: Attribute = AttributeReference("TableName", StringType)(exprId = ExprId(0, UUID.randomUUID()))

  def generateCatalogStatistics(sparkSession: SparkSession, nameCode: Int, logicalPlan: LogicalPlan, fraction: Double): MjStatistics = {
    val df = Dataset.ofRows(sparkSession, logicalPlan)
    val schema = df.schema
    val rdd = df.rdd
    val p = if (p0 < 4) 4 else p0
    val sp = 0
    val sampler: RandomSampler[Row, Row] = new BernoulliSampler[Row](fraction)
    val partitions = getPartitions(rdd)
    val parts = rdd.mapPartitionsWithIndex{
      case (pid, iter)=> {
        // 采样+count+size+HLL
        // 每个Partition内的size
        var s: Long = 0
        // 每个Partition内的count
        var c: Long = 0
        // key是field name, value是该field对应的cardinality
        val cards: Array[HyperLogLogPlus] = new Array[HyperLogLogPlus](schema.length).map(_ => new HyperLogLogPlus(p, sp))

        // 进行Bernoulli采样
        val split = partitions(pid).asInstanceOf[PartitionwiseSampledRDDPartition]
        val thisSampler = sampler.clone
        thisSampler.setSeed(split.seed)

        val splitSample: mutable.ArrayBuffer[Row] = mutable.ArrayBuffer[Row]()
        for (r <- iter) {
          c += 1
          s += r.size
          // 合并HyperLogLogPlus
          for (f <- schema.fields) {
            val idx = r.fieldIndex(f.name)
            cards(idx).offer(r.get(idx))
          }
          //Bernoulli采样
          if (thisSampler.sample() > 0) splitSample.append(r)
        }
        Iterator(PartitionInfo(s, c, cards, splitSample.toIterator))
      }
    }
    val cols = schema.fields.map (_.name)
    val sample: joinery.DataFrame[Any] = new joinery.DataFrame[Any](cols:_*)
    parts.flatMap(i => i.sample).collect().foreach {
      case r =>
        val list = java.util.Arrays.asList(cols.map(f => r.getAs[Any](f)))
        sample.append(list)
    }

     val metaInfo = parts.map(i => (i.size, i.count, i.cards))
      .reduce((l, r)=>(l._1+r._1, l._2+r._2,
        l._3.zip(r._3)
          .map{
            case(lh: HyperLogLogPlus, rh: HyperLogLogPlus) => {
              lh.addAll(rh)
              lh
            }}
        )
      )

    val size = metaInfo._1
    val count = metaInfo._2
    val cardinality = schema.fields.map(_.name).zip(metaInfo._3.map(_.cardinality())).toMap
    //TODO: 这里没有统计最大值, 最小值(用None填充), 以及tuple的平均长度和最大长度(用8填充)
    val colStats = cardinality.map {
      case (k, v) =>
        k -> ColumnStat(v, None, None, 0, 8, 8)
    }
    val stat: ColumnStat = ColumnStat(distinctCount = 0, nullCount = 0, min = None, max = Some(nameCode), avgLen = 0, maxLen = 0)
    val matched = logicalPlan.output
      .flatMap(a => colStats.get(a.name).map(a -> _)) :+ tableName -> stat

    // attribute信息中增加一列, 列名为TableName, max为其表名
    val attributeStats: AttributeMap[ColumnStat] = AttributeMap(matched)
    MjStatistics(sizeInBytes = size, rowCount = Some(count), attributeStats = attributeStats,
      sample = sample, fraction = fraction)
  }

  def getPartitions(rdd: RDD[Row]): Array[Partition] = {
    val random = new Random(seed)
    rdd.partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
  }
}
