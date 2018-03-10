package org.pasalab.automj

/**
 * Created by wuxiaoqi on 17-11-28.
 */
object MjConfigConst {
  val METADATA_LOCATION: String = "spark.automj.metadata.location"
  val MULTI_ROUND_STRATEGY: String = "spark.automj.multiRoundStrategy"
  val ONE_ROUND_STRATEGY: String = "spark.automj.oneRoundStrategy"
  val JOIN_SIZE_ESTIMATOR: String = "spark.automj.joinSizeEstimator"
  val JOIN_DEFAULT_SIZE: String = "spark.automj.joinDefaultSize"
  val SHARE_JOIN_DEFAULT_SIZE: String = "spark.automj.oneRound.joinDefaultSize"
  val Force_ONE_ROUND: String = "spark.automj.useOneRound"
  val ENABLE_STATISTICS: String = "spark.automj.statistics.enable"
  val SAMPLE_FRACTION: String = "spark.automj.statistics.sample.fraction"
  val ONE_ROUND_ONCE: String = "spark.automj.oneRound.once"
  val ONE_ROUND_PARTITIONS: String = "spark.automj.oneRound.partitions"
  val DATA_SCALA: String = "spark.automj.data.scale"
  val EXECUTION_MODE: String = "spark.automj.execution.mode"
}
