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

import com.hortonworks.spark.atlas.sql.SparkCatalogEventTracker
import com.hortonworks.spark.atlas.{AtlasClientConf, BaseResourceIT, RestAtlasClient}
import org.apache.spark.ml._
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.Matchers


class MLPipeLineEventTrackerSuite extends BaseResourceIT with Matchers{

  private var sparkSession: SparkSession = _

  private var tracker: MLPipelineTracker = _

  protected  override  val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
    .set(AtlasClientConf.ATLAS_REST_ENDPOINT.key, "http://172.27.9.150:21000")

  private val atlasClient = new RestAtlasClient(atlasClientConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
    tracker = new MLPipelineTracker(atlasClient, atlasClientConf)
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
  }

  test("Pipeline create event to Atlas entities") {

    val df = sparkSession.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    )).toDF("id", "features", "label")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
      .setMin(0.0)
      .setMax(3.0)
    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(df)

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    pipeline.write.overwrite().save(pipelineDir)

    model.write.overwrite().save(modelDir)

    val df2 = model.transform(df)
    df2.collect()





    tracker.onOtherEvent(CreatePipelineEvent(pipeline, df))
    tracker.onOtherEvent(CreateModelEvent(model))
    tracker.onOtherEvent(SavePipelineEvent(pipelineDir))
    tracker.onOtherEvent(SaveModelEvent(modelDir))
    tracker.onOtherEvent(TransformEvent(model, df))
  }

}
