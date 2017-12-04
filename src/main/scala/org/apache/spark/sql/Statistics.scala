package org.apache.spark.sql

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.Partition
import org.apache.spark.rdd.PartitionwiseSampledRDDPartition
import org.apache.spark.util.random.{BernoulliSampler, RandomSampler}
import org.pasalab.automj.PartitionInfo

import scala.collection.mutable
import scala.util.Random

/**
 * Created by wuxiaoqi on 17-11-28.
 */
class Statistics(dataFrame: DataFrame,
                 sqlContext: SQLContext,
                 fraction: Double,
                 @transient private val seed: Long = new Random().nextLong,
                 relativeSD: Double = 0.05) {
  val schema = dataFrame.schema
  val rdd = dataFrame.rdd
  val sampler: RandomSampler[Row, Row] = new BernoulliSampler[Row](fraction)
  private var size: Long = 0
  private var count: Long = 0
  private var cardinality: Map[String, Long] = null
  private var sample: DataFrame = null
  private val p0 = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt

  def compute: Unit = {
    val p = if (p0 < 4) 4 else p0
    val sp = 0
    val partitions = getPartitions
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
    val sampleRDD = parts.flatMap(i => i.sample)
    sample = sqlContext.createDataFrame(sampleRDD, schema)

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
    size = metaInfo._1
    count = metaInfo._2
    cardinality = schema.fields.map(_.name).zip(metaInfo._3.map(_.cardinality())).toMap
  }
  def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    rdd.partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
  }
  def getSize: Long = {
    if (size == 0) compute
    size
  }
  def getCount: Long = {
    if (count == 0) compute
    count
  }
  def getCardinality: Map[String, Long] = {
    if (cardinality == null) compute
    cardinality
  }
  def getSample: DataFrame = {
    if (sample == null) compute
    sample
  }
}
