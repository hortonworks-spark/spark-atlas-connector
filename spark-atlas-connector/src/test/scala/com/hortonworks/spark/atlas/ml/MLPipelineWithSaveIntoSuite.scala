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

import java.io.File
import java.util

import scala.util.Random
import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.commons.io.FileUtils
import org.apache.atlas.model.instance.AtlasEntity

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand

import com.hortonworks.spark.atlas.BaseResourceIT
import com.hortonworks.spark.atlas.sql.{CommandsHarvester, QueryDetail}
import com.hortonworks.spark.atlas.types.internal


class MLPipelineWithSaveIntoSuite extends BaseResourceIT with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private val dataDir1 = "target/dir1/orc/"
  private val dataDir2 = "target/dir2/orc/"
  private val destinationSparkTblName = "destination_s_" + Random.nextInt(100000)
  private val destinationSparkTblName2 = "destination_s_" + Random.nextInt(100000)
  private val sourceSparkTblName = "source_s_" + Random.nextInt(100000)
  private val sourceSparkTblName2 = "source_s_" + Random.nextInt(100000)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    val input = Seq(
        (1, "abc test1 123"),
        (2, "edf test2 345"),
        (3, "xyz test3 567"),
        (4, "wzx test4 789 "))

    val df = sparkSession.createDataFrame(input).toDF("id", "text")
    df.write.mode(SaveMode.Overwrite).format("orc").save(dataDir1)

    val createCommand1 = s"CREATE TABLE IF NOT EXISTS ${sourceSparkTblName} " +
      s" (id int, text string) USING orc LOCATION '${dataDir1}'"
    sparkSession.sql(createCommand1)

    val createCommand2 = s"CREATE TABLE IF NOT EXISTS ${sourceSparkTblName2} " +
      s" (text string) USING orc LOCATION '${dataDir2}'"
    sparkSession.sql(createCommand2)

    sparkSession.sql(s"CREATE TABLE $destinationSparkTblName (text string) USING ORC")
    sparkSession.sql(s"CREATE TABLE $destinationSparkTblName2 (text string) USING ORC")
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null

    FileUtils.deleteDirectory(new File("metastore_db"))
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File(dataDir1))
    internal.cachedObjects.clear()

    super.afterAll()
  }

  test("ML Pipeline and Model Save into place..") {
    // Test for the DataFrame operation
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationSparkTblName " +
      s"SELECT text FROM $sourceSparkTblName").queryExecution

    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

    val entities = CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    val pUid1 = pEntity.getGuid
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)
    val inputTab = inputs.asScala.head

    // Test for the model training process
    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)

    val trainData = sparkSession.read.format("orc").load(dataDir1)
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val pipeline = new Pipeline().setStages(Array(tokenizer))
    val model = pipeline.fit(trainData)
    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
    val modelEntity = internal.mlModelToEntity(model, modelDirEntity)

    val logMap = Map("sparkPlanDescription" ->
      (s"Spark ML training model with pipeline uid: ${pipeline.uid}"))

    val processEntity = internal.etlProcessToEntity(
      List(pipelineEntity, inputTab), List(modelEntity), logMap)

    val pUid2 = processEntity.getGuid
    // DF process and ML process should be the same process ID
    pUid1 should be equals pUid2
  }

  test("ML training and scoring, then saving result into physical storage.") {
    // Test for the DataFrame operation
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationSparkTblName " +
      s"SELECT text FROM $sourceSparkTblName").queryExecution

    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

    val entities = CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(cmd, qd)
    val pEntity1 = entities.head

    val inputs = pEntity1.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)
    val inputTab = inputs.asScala.head

    // Test for the model training process
    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)

    val trainData = sparkSession.read.format("orc").load(dataDir1)
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val pipeline = new Pipeline().setStages(Array(tokenizer))
    val model = pipeline.fit(trainData)
    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
    val modelEntity = internal.mlModelToEntity(model, modelDirEntity)

    val logMap = Map("sparkPlanDescription" ->
      (s"Spark ML training model with pipeline uid: ${pipeline.uid}"))

    val fitProcessEntity = internal.etlProcessToEntity(
      List(pipelineEntity, inputTab), List(modelEntity), logMap)

    pEntity1.getGuid should be equals fitProcessEntity.getGuid

    internal.cachedObjects.put("fit_process", fitProcessEntity.getGuid)

    // Test for data transform process
    val df2 = model.transform(trainData)
    val uid = model.uid
    internal.cachedObjects.put("model_uid", uid)
    internal.cachedObjects.put(s"${uid}_modelDirEntity", modelDirEntity)
    internal.cachedObjects.put(s"${uid}_modelEntity", modelEntity)

    df2.select("text").write.mode(SaveMode.Overwrite).format("orc").save(dataDir2)

    val qe2 = sparkSession.sql(s"INSERT INTO TABLE $destinationSparkTblName2 " +
      s"SELECT text FROM $sourceSparkTblName2").queryExecution

    assert(qe2.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node2 = qe2.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node2.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand])
    val cmd2 = node2.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

    val qd2 = QueryDetail(qe, 0L, 0L)
    val entities2 = CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(cmd2, qd2)
    val pEntity2 = entities2.head

    // This input is the output of ML scoring job
    val inputs2 = pEntity2.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs2.size() should be (1)

    val logMap2 = Map("sparkPlanDescription" ->
      (s"Spark ML scoring model with pipeline uid: ${pipeline.uid} and model uid: ${model.uid}"))

    val entities3 = internal.updateMLProcessToEntity(List(inputs.asScala.head),
      List(inputs2.asScala.head), logMap2)
    val pEntity3 = entities3.head

    pEntity2.getGuid should be equals pEntity3.getGuid

    internal.cachedObjects.clear()
  }
}
