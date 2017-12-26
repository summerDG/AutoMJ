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

import org.apache.spark.sql.automj.MjSessionCatalog

import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.pasalab.automj.MjConfigConst

/**
 * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
 */
trait SharedSQLContext extends SQLTestUtils with BeforeAndAfterEach with Eventually {

  protected def sparkConf = {
    new SparkConf().set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set(MjConfigConst.ONE_ROUND_STRATEGY, "org.pasalab.automj.ShareStrategy")
      .set(MjConfigConst.MULTI_ROUND_STRATEGY, "org.pasalab.automj.LeftDepthStrategy")
      .set(MjConfigConst.JOIN_SIZE_ESTIMATOR, "org.pasalab.automj.EstimatorBasedSample")
      .set(MjConfigConst.ONE_ROUND_ONCE, "true")
      .set(MjConfigConst.SAMPLE_FRACTION, "1.0")
  }

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _spark: TestSparkSession = null

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def spark: SparkSession = _spark

  protected implicit def catalog: MjSessionCatalog = _spark.sessionState.catalog.asInstanceOf[MjSessionCatalog]

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
    new TestSparkSession(sparkConf)
  }

  /**
   * Initialize the [[TestSparkSession]].
   */
  protected override def beforeAll(): Unit = {
    SparkSession.sqlListener.set(null)
    if (_spark == null) {
      _spark = createSparkSession
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // files can be closed from other threads, so wait a bit
    // normally this doesn't take more than 1s
    eventually(timeout(10.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }
}
