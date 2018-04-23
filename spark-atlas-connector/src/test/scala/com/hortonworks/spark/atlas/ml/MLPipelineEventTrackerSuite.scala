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

package com.hortonworks.spark.atlas.ml

import com.hortonworks.spark.atlas.BaseResourceIT
import com.hortonworks.spark.atlas.types.internal
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.Matchers

import scala.collection.mutable

class MLPipelineEventTrackerSuite extends BaseResourceIT with Matchers {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    Thread.sleep(5000L)
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    internal.cachedObjects.clear()
    sparkSession = null
  }

  def testWithPipeline(name: String)(
    f: (Pipeline, Seq[MLListenEvent] => Unit) => Unit): Unit = test(name) {

    val recorder = mutable.Buffer.empty[MLListenEvent]

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
      .setMin(0.0)
      .setMax(3.0)

    val pipeline = new Pipeline().setStages(Array(scaler))

    pipeline.addListener(new MLListener {
      override def onEvent(event: MLListenEvent): Unit = {
        recorder += event
      }
    })

    f(pipeline, (expected: Seq[MLListenEvent]) => {
      val actual = recorder.clone()
      recorder.clear()
      assert(expected === actual)
    })
  }

  testWithPipeline("pipelinetracker_fitData") { (newPipeline, checkEvents) =>

    val pipelineDir = "tmp/pipeline1"

    val df = sparkSession.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    )).toDF("id", "features", "label")

    val model = newPipeline.fit(df)

    checkEvents(CreatePipelineEvent(newPipeline, df) :: CreateModelEvent(model) :: Nil)

  }

}
