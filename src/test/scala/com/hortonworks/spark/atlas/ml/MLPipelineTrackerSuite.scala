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

import com.hortonworks.spark.atlas.{AtlasClientConf, RestAtlasClient}
import com.hortonworks.spark.atlas.types._
import com.hortonworks.spark.atlas.TestUtils._
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.Matchers

class MLPipelineTrackerSuite extends SparkFunSuite with Matchers {

  private var sparkSession: SparkSession = _
  private val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
    .set(AtlasClientConf.ATLAS_REST_ENDPOINT.key, "http://172.27.65.212:21000")
  private val altasClient = new RestAtlasClient(atlasClientConf)

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

  def getTableEntity(tableName: String): (AtlasEntity, List[AtlasEntity], AtlasEntity, AtlasEntity) = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/yliang")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("features", StringType, false)
      .add("label", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)

    val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)
    val sdEntity = AtlasEntityUtils.storageFormatToEntity(sd, "db1", s"$tableName")
    val schemaEntities = AtlasEntityUtils.schemaToEntity(schema, "db1", s"$tableName")
    val tableEntity =
      AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity, schemaEntities, sdEntity)
    (dbEntity, schemaEntities, sdEntity, tableEntity)
  }

  test("pipeline and pipeline model") {

    SparkAtlasModel.checkAndCreateTypes(altasClient)

    val uri = "/"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = AtlasEntityUtils.MLDirectoryToEntity(uri, pipelineDir)
    val modelDirEntity = AtlasEntityUtils.MLDirectoryToEntity(uri, modelDir)

//    println(pipelineDirEntity.getGuid)
//    println(modelDirEntity.getGuid)
//    println("-------")
    altasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity))

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

    val fitStartTime = System.nanoTime()
    val model = pipeline.fit(df)
    val fitEndTime = System.nanoTime()

    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = AtlasEntityUtils.MLPipelineToEntity(pipeline, pipelineDirEntity)

//    println(pipelineEntity.getGuid)
//    println("-------")
    altasClient.createEntities(Seq(pipelineDirEntity, pipelineEntity))

    val modelEntity = AtlasEntityUtils.MLModelToEntity(model, modelDirEntity)

//    println(modelEntity.getGuid)
//    println("-------")
    altasClient.createEntities(Seq(modelDirEntity, modelEntity))

    val (dbEntity1, schemaEntities1, sdEntity1, tableEntity1) = getTableEntity("yliang1")
    val (dbEntity2, schemaEntities2, sdEntity2, tableEntity2) = getTableEntity("yliang2")

//    println(dbEntity1.getGuid)
//    println(sdEntity1.getGuid)
//    println(tableEntity1.getGuid)
//    println(schemaEntities1.map(_.getGuid))
//    println("-------")

    altasClient.createEntities(Seq(dbEntity1, sdEntity1, tableEntity1) ++ schemaEntities1)
    altasClient.createEntities(Seq(dbEntity2, sdEntity2, tableEntity2) ++ schemaEntities2)

    val fitEntity = AtlasEntityUtils.MLFitProcessToEntity(
      pipeline, pipelineEntity, fitStartTime, fitEndTime, List(tableEntity1), List(modelEntity))

//    println(pipelineEntity.getGuid)
//    println(modelEntity.getGuid)
//    println(fitEntity.getGuid)
//    println("-------")

    altasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity, pipelineEntity, modelEntity,
      dbEntity1, sdEntity1, tableEntity1, fitEntity) ++ schemaEntities1)

    model.write.overwrite().save(modelDir)

    val transformStartTime = System.nanoTime()
    val df2 = model.transform(df)
    df2.collect()
    val transformEndTime = System.nanoTime()

    val transformEntity = AtlasEntityUtils.MLTransformProcessToEntity(
      model, modelEntity, transformStartTime, transformEndTime, List(tableEntity1), List(tableEntity2))


    altasClient.createEntities(Seq(modelDirEntity, modelEntity, dbEntity1, sdEntity1, tableEntity1,
      dbEntity2, sdEntity2, tableEntity2, transformEntity) ++ schemaEntities1 ++ schemaEntities2)
  }
}