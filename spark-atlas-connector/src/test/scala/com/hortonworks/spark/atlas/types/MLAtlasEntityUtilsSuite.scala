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

import java.io.File

import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.TestUtils._
import com.hortonworks.spark.atlas.{AtlasUtils, WithHiveSupport}

class MLAtlasEntityUtilsSuite extends FunSuite with Matchers with WithHiveSupport {

  def getTableEntity(tableName: String): AtlasEntity = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)

    val tableEntities = internal.sparkTableToEntity(
      tableDefinition, AtlasConstants.DEFAULT_CLUSTER_NAME, Some(dbDefinition))
    val tableEntity = tableEntities.entity

    tableEntity
  }

  test("pipeline, pipeline model, fit and transform") {
    val uri = "/"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    pipelineDirEntity.entity.getAttribute("uri") should be (uri)
    pipelineDirEntity.entity.getAttribute("directory") should be (pipelineDir)
    pipelineDirEntity.dependencies.length should be (0)

    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)
    modelDirEntity.entity.getAttribute("uri") should be (uri)
    modelDirEntity.entity.getAttribute("directory") should be (modelDir)
    modelDirEntity.dependencies.length should be (0)

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

    val pipelineEntity = internal.mlPipelineToEntity(pipeline.uid, pipelineDirEntity)
    pipelineEntity.entity.getTypeName should be (metadata.ML_PIPELINE_TYPE_STRING)
    pipelineEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      pipeline.uid)
    pipelineEntity.entity.getAttribute("name") should be (pipeline.uid)
    pipelineEntity.entity.getAttribute("directory") should be (
      AtlasUtils.entityToReference(pipelineDirEntity.entity, useGuid = false))
    pipelineEntity.dependencies should be (Seq(pipelineDirEntity))

    val modelEntity = internal.mlModelToEntity(model.uid, modelDirEntity)
    val modelUid = model.uid.replaceAll("pipeline", "model")
    modelEntity.entity.getTypeName should be (metadata.ML_MODEL_TYPE_STRING)
    modelEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (modelUid)
    modelEntity.entity.getAttribute("name") should be (modelUid)
    modelEntity.entity.getAttribute("directory") should be (
      AtlasUtils.entityToReference(modelDirEntity.entity, useGuid = false))

    modelEntity.dependencies should be (Seq(modelDirEntity))

    FileUtils.deleteDirectory(new File("tmp"))
  }
}
