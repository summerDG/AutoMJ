package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * Created by wuxiaoqi on 17-12-4.
 */
class ZippedPartitionsRDDs[A: ClassTag, V: ClassTag](
                            sc: SparkContext,
                            var f: Seq[Iterator[A]] => Iterator[V],
                            var head: RDD[A],
                            var tail: Seq[RDD[A]],
                            preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, head +: tail, preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f((head +: tail).zip(partitions).map {
      case (rdd, partition) => rdd.iterator(partition, context)
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    head = null
    tail = null
    f = null
  }
}
object ZippedPartitionsRDDs {
  def apply[A: ClassTag, V: ClassTag](sc: SparkContext, head: RDD[A], tail: Seq[RDD[A]])
                                     (f: (Seq[Iterator[A]] => Iterator[V])): RDD[V] = RDDOperationScope.withScope(sc)(
    new ZippedPartitionsRDDs(sc, sc.clean(f), head, tail, true)
  )
}