/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{MjSession, SparkSession}
import org.apache.spark.sql.internal.{SQLConf, SessionState, SessionStateBuilder, WithTestConf}
import org.pasalab.automj.MjConfigConst

/**
 * A special `SparkSession` prepared for testing.
 */
private[sql] class TestSparkSession(sc: SparkContext) extends MjSession(sc) { self =>
  def this(sparkConf: SparkConf) {
    this(new SparkContext("local[2]", "test-sql-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set(MjConfigConst.ONE_ROUND_STRATEGY, "org.pasalab.automj.ShareStrategy")
        .set(MjConfigConst.MULTI_ROUND_STRATEGY, "org.pasalab.automj.LeftDepthStrategy")
        .set(MjConfigConst.JOIN_SIZE_ESTIMATOR, "org.pasalab.automj.EstimatorBasedSample")
        .set(MjConfigConst.ONE_ROUND_PARTITIONS, "8")
        .set(MjConfigConst.DATA_SCALA, "2")
        .set(MjConfigConst.EXECUTION_MODE, "mixed")
    ))
  }

  def this() {
    this(new SparkConf)
  }
}


private[sql] object TestSQLContext {

  /**
   * A map used to store all confs that need to be overridden in sql/core unit tests.
   */
  val overrideConfs: Map[String, String] =
  Map(
    // Fewer shuffle partitions to speed up testing.
    SQLConf.SHUFFLE_PARTITIONS.key -> "8")
}

private[sql] class TestSQLSessionStateBuilder(
                                               session: SparkSession,
                                               state: Option[SessionState])
  extends SessionStateBuilder(session, state) with WithTestConf {
  override def overrideConfs: Map[String, String] = TestSQLContext.overrideConfs
  override def newBuilder: NewBuilder = new TestSQLSessionStateBuilder(_, _)
}