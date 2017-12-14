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

package com.hortonworks.spark.atlas.types

import com.hortonworks.spark.atlas.TestUtils._
import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.Matchers

class MLAtlasEntityUtilsSuite extends SparkFunSuite with Matchers with MLlibTestSparkContext {

  import testImplicits._

  def getTableEntity(tableName: String): AtlasEntity = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)

    val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)
    val sdEntity = AtlasEntityUtils.storageFormatToEntity(sd, "db1", s"$tableName")
    val schemaEntities = AtlasEntityUtils.schemaToEntity(schema, "db1", s"$tableName")
    val tableEntity =
      AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity, schemaEntities, sdEntity)
    tableEntity
  }

  test("pipeline, pipeline model, fit and transform") {
    val uri = "/"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = AtlasEntityUtils.MLDirectoryToEntity(uri, pipelineDir)
    pipelineDirEntity.getAttribute("uri") should be (uri)
    pipelineDirEntity.getAttribute("directory") should be (pipelineDir)

    val modelDirEntity = AtlasEntityUtils.MLDirectoryToEntity(uri, modelDir)
    modelDirEntity.getAttribute("uri") should be (uri)
    modelDirEntity.getAttribute("directory") should be (modelDir)

    val df = Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    ).toDF("id", "features", "label")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
      .setMin(0.0)
      .setMax(3.0)
    val pipeline = new Pipeline().setStages(Array(scaler))

    val fitStartTime = System.nanoTime()
    val model = pipeline.fit(df)
    val fitEndTime = System.nanoTime()

    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = AtlasEntityUtils.MLPipelineToEntity(pipeline, pipelineDirEntity)
    pipelineEntity.getTypeName should be (metadata.ML_PIPELINE_TYPE_STRING)
    pipelineEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (pipeline.uid)
    pipelineEntity.getAttribute("name") should be (pipeline.uid)
    pipelineEntity.getAttribute("directory") should be (pipelineDirEntity)

    val modelEntity = AtlasEntityUtils.MLModelToEntity(model, modelDirEntity)
    val modelUid = model.uid.replaceAll("pipeline", "pipeline_model")
    modelEntity.getTypeName should be (metadata.ML_MODEL_TYPE_STRING)
    modelEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (modelUid)
    modelEntity.getAttribute("name") should be (modelUid)
    modelEntity.getAttribute("directory") should be (modelDirEntity)

    val inputEntity1 = getTableEntity("tbl1")
    val inputEntity2 = getTableEntity("tbl2")
    val fitEntity = AtlasEntityUtils.MLFitProcessToEntity(
      pipeline, pipelineEntity, fitStartTime, fitEndTime, List(inputEntity1), List(modelEntity))

    val fitName = s"${pipeline.uid}_$fitStartTime"
    fitEntity.getTypeName should be (metadata.ML_FIT_PROCESS_TYPE_STRING)
    fitEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (fitName)
    fitEntity.getAttribute("name") should be (fitName)
    fitEntity.getAttribute("pipeline") should be (pipelineEntity)
    fitEntity.getAttribute("startTime") should be (fitStartTime)
    fitEntity.getAttribute("endTime") should be (fitEndTime)

    model.write.overwrite().save(modelDir)

    val transformStartTime = System.nanoTime()
    val df2 = model.transform(df)
    df2.collect()
    val transformEndTime = System.nanoTime()

    val transformEntity = AtlasEntityUtils.MLTransformProcessToEntity(
      model, modelEntity, transformStartTime, transformEndTime, List(inputEntity1), List(inputEntity2))

    val transformName = s"${model.uid}_$transformStartTime"
    transformEntity.getTypeName should be (metadata.ML_TRANSFORM_PROCESS_TYPE_STRING)
    transformEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (transformName)
    transformEntity.getAttribute("name") should be (transformName)
    transformEntity.getAttribute("model") should be (modelEntity)
    transformEntity.getAttribute("startTime") should be (transformStartTime)
    transformEntity.getAttribute("endTime") should be (transformEndTime)
  }
}