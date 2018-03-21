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

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import com.hortonworks.spark.atlas.{AtlasClientConf, BaseResourceIT, RestAtlasClient}
import com.hortonworks.spark.atlas.types._
import com.hortonworks.spark.atlas.TestUtils._

class MLPipelineTrackerIT extends BaseResourceIT with Matchers with BeforeAndAfterAll {
  private var sparkSession: SparkSession = _
  private val atlasClient = new RestAtlasClient(atlasClientConf)

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
  }

  // Return table related entities as a Sequence.
  // The first one is table entity, followed by
  // db entity, storage entity and schema entities.
  def getTableEntities(tableName: String): Seq[AtlasEntity] = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)
    val tableEntities = internal.sparkTableToEntities(tableDefinition, Some(dbDefinition))

    tableEntities
  }

  // Enable it to run integrated test.
  it("pipeline and pipeline model") {
    SparkAtlasModel.checkAndCreateTypes(atlasClient)

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)

    atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity))

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

    atlasClient.createEntities(Seq(pipelineDirEntity, pipelineEntity))

    val modelEntity = internal.mlModelToEntity(model, modelDirEntity)

    atlasClient.createEntities(Seq(modelDirEntity, modelEntity))

    val tableEntities1 = getTableEntities("chris1")
    val tableEntities2 = getTableEntities("chris2")

    atlasClient.createEntities(tableEntities1)
    atlasClient.createEntities(tableEntities2)

    val fitEntity = internal.mlFitProcessToEntity(
      pipeline,
      pipelineEntity,
      List(pipelineEntity, tableEntities1.head),
      List(modelEntity))

    atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity,
      pipelineEntity, modelEntity, fitEntity) ++ tableEntities1)

    model.write.overwrite().save(modelDir)

    val df2 = model.transform(df)
    df2.collect()

    val transformEntity = internal.mlTransformProcessToEntity(
      model,
      modelEntity,
      List(modelEntity, tableEntities1.head),
      List(tableEntities2.head))

    atlasClient.createEntities(Seq(modelDirEntity, modelEntity, transformEntity)
      ++ tableEntities1 ++ tableEntities2)
  }
}
