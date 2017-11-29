package org.pasalab.automj

import org.apache.spark.Partition

/**
 * Created by wuxiaoqi on 17-11-28.
 */
class HcPartition extends Partition{
  override def index: Int = ???
}
