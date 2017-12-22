package org.pasalab.automj

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.sql.Row

/**
 * Created by wuxiaoqi on 17-11-29.
 */
case class PartitionInfo(size: BigInt, count: BigInt, cards: Array[HyperLogLogPlus], sample: Iterator[Row])
