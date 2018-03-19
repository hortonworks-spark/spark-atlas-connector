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

import scala.concurrent.duration._
import com.hortonworks.spark.atlas.types.{SparkAtlasModel, internal, metadata}
import com.hortonworks.spark.atlas.{AtlasClientConf, BaseResourceIT, RestAtlasClient}
import org.apache.spark
import org.apache.spark.ml._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._


class MLPipeLineEventTrackerEntitySuite extends BaseResourceIT with Matchers{

  private var sparkSession: SparkSession = _

  private var tracker: MLPipelineTracker = _

  protected  override  val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
    .set(AtlasClientConf.ATLAS_REST_ENDPOINT.key, "http://172.27.69.1:21000")

  private val atlasClient = new RestAtlasClient(atlasClientConf)
  SparkAtlasModel.checkAndCreateTypes(atlasClient)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
    tracker = new MLPipelineTracker(atlasClient, atlasClientConf)
  }

  override def afterAll(): Unit = {
    Thread.sleep(5000L)
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
  }


  test("Spark ML Pipeline save event to Atlas Entity ") {

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline1"
    val training = sparkSession.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline().setStages(Array(lr))

    val model = pipeline.fit(training)

    pipeline.write.overwrite().save(pipelineDir)

    tracker.onOtherEvent(CreatePipelineEvent(pipeline, training))
    tracker.onOtherEvent(SavePipelineEvent(pipeline.uid, pipelineDir))
  }

  test("Spark PipelineModel save event to Atlas Entity")
  {

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline2"
    val modelDir = "tmp/model2"

    val training = sparkSession.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline().setStages(Array(lr))

    val model = pipeline.fit(training)

    pipeline.write.overwrite().save(pipelineDir)
    model.write.overwrite().save(modelDir)

    tracker.onOtherEvent(CreatePipelineEvent(pipeline, training))
    tracker.onOtherEvent(SavePipelineEvent(pipeline.uid, pipelineDir))
    tracker.onOtherEvent(CreateModelEvent(model))
    tracker.onOtherEvent(SaveModelEvent(model.uid, modelDir))
  }


  test("Spark PipelineModel transform event  to Atlas entities") {

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline4"
    val modelDir = "tmp/model4"

    val training = sparkSession.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline().setStages(Array(lr))

    val model = pipeline.fit(training)

    pipeline.write.overwrite().save(pipelineDir)

    model.write.overwrite().save(modelDir)

    val savedmodel = PipelineModel.load(modelDir)

    val testing = sparkSession.read.format("libsvm").load("data/mllib/sample_libsvm_data_test.txt")

    val df2 = savedmodel.transform(testing)

    df2.show(10)

    tracker.onOtherEvent(CreatePipelineEvent(pipeline, training))
    tracker.onOtherEvent(CreateModelEvent(model))
    tracker.onOtherEvent(SavePipelineEvent(pipeline.uid, pipelineDir))
    tracker.onOtherEvent(SaveModelEvent(model.uid, modelDir))
    tracker.onOtherEvent(LoadModelEvent(modelDir, savedmodel))
    tracker.onOtherEvent(TransformEvent(savedmodel, testing, df2))
  }

}
