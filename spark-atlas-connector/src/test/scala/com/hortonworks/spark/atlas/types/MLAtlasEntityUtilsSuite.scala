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

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.TestUtils._

class MLAtlasEntityUtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
    super.afterAll()
  }

  def getTableEntity(tableName: String): AtlasEntity = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)

    val tableEntities = internal.sparkTableToEntities(tableDefinition, Some(dbDefinition))
    val tableEntity = tableEntities.head

    tableEntity
  }

  test("pipeline, pipeline model, fit and transform") {
    val uri = "/"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    pipelineDirEntity.getAttribute("uri") should be (uri)
    pipelineDirEntity.getAttribute("directory") should be (pipelineDir)

    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)
    modelDirEntity.getAttribute("uri") should be (uri)
    modelDirEntity.getAttribute("directory") should be (modelDir)

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

    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
    pipelineEntity.getTypeName should be (metadata.ML_PIPELINE_TYPE_STRING)
    pipelineEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (pipeline.uid)
    pipelineEntity.getAttribute("name") should be (pipeline.uid)
    pipelineEntity.getAttribute("directory") should be (pipelineDirEntity)

    val modelEntity = internal.mlModelToEntity(model, modelDirEntity)
    val modelUid = model.uid.replaceAll("pipeline", "model")
    modelEntity.getTypeName should be (metadata.ML_MODEL_TYPE_STRING)
    modelEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (modelUid)
    modelEntity.getAttribute("name") should be (modelUid)
    modelEntity.getAttribute("directory") should be (modelDirEntity)

    val inputEntity1 = getTableEntity("tbl1")
    val inputEntity2 = getTableEntity("tbl2")
    val fitEntity = internal.mlFitProcessToEntity(
      pipeline, pipelineEntity, List(inputEntity1, pipelineEntity), List(modelEntity))

    val fitName = pipeline.uid.replaceAll("pipeline", "fit_process")
    fitEntity.getTypeName should be (metadata.ML_FIT_PROCESS_TYPE_STRING)
    fitEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (fitName)
    fitEntity.getAttribute("name") should be (fitName)
    fitEntity.getAttribute("pipeline") should be (pipelineEntity)

    model.write.overwrite().save(modelDir)

    val df2 = model.transform(df)
    df2.collect()

    val transformEntity = internal.mlTransformProcessToEntity(
      model, modelEntity, List(inputEntity1, modelEntity), List(inputEntity2))

    val transformName = model.uid.replaceAll("pipeline", "transform_process")
    transformEntity.getTypeName should be (metadata.ML_TRANSFORM_PROCESS_TYPE_STRING)
    transformEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (transformName)
    transformEntity.getAttribute("name") should be (transformName)
    transformEntity.getAttribute("model") should be (modelEntity)
  }
}
