package org.pasalab.automj

import org.apache.spark.Partition

/**
 * Created by wuxiaoqi on 17-11-29.
 */
class PartitionwiseSampledRDDPartition(val prev: Partition, val seed: Long)
  extends Partition with Serializable {
  override val index: Int = prev.index
}
