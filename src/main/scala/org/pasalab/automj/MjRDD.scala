package org.pasalab.automj

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.random.RandomSampler

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by wuxiaoqi on 17-11-28.
 */
class MjRDD[T: ClassTag, U: ClassTag](prev: RDD[T],
            sampler: RandomSampler[T, U],
            preservesPartitioning: Boolean,
            @transient private val seed: Long = new Random().nextLong) extends RDD[T](prev){

}
