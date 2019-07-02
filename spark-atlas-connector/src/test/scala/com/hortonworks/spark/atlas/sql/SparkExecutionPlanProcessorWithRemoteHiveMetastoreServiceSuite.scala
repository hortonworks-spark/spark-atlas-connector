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

package com.hortonworks.spark.atlas.sql

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.Files

import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.sql.testhelper._
import com.hortonworks.spark.atlas.types.external
import org.apache.atlas.model.instance.AtlasObjectId
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExecutedCommandExec}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import scala.util.Random

class SparkExecutionPlanProcessorWithRemoteHiveMetastoreServiceSuite
  extends FunSuite
  with BeforeAndAfterEach
  with Matchers
  with WithRemoteHiveMetastoreServiceSupport
  with ProcessEntityValidator
  with TableEntityValidator
  with FsEntityValidator {
  import com.hortonworks.spark.atlas.AtlasEntityReadHelper._

  private val sourceTblName = "source_" + Random.nextInt(100000)

  val brokerProps: Map[String, Object] = Map[String, Object]()
  var kafkaTestUtils: KafkaTestUtils = _

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  val clusterName: String = atlasClientConf.get(AtlasClientConf.CLUSTER_NAME)

  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $dbName.$sourceTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $dbName.$sourceTblName VALUES ('a'), ('b'), ('c')")

    atlasClient = new CreateEntitiesTrackingAtlasClient()
    testHelperQueryListener.clear()
    sparkSession.listenerManager.register(testHelperQueryListener)
  }

  override def afterAll(): Unit = {
    atlasClient = null
    sparkSession.listenerManager.unregister(testHelperQueryListener)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    atlasClient.clearEntities()
  }

  test("CREATE EXTERNAL TABLE ... LOCATION ...") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)
    val tempDir = Files.createTempDirectory("spark-atlas-connector-temp")

    val rand = new scala.util.Random()
    val outputTableName = "employee_details_" + rand.nextInt(1000000000)

    sparkSession.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $dbName.$outputTableName " +
      "(name STRING, age INT, emp_id INT, designation STRING) " +
      s"LOCATION '$tempDir'")

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    // The query doesn't bring spark_process entity - it only brings table entities
    // which SAC will create reference for table entity when Spark connects to remote HMS.
    // SAC Atlas Client doesn't request creation for reference, hence NO recorded entities.
    assert(entities.length === 0)
  }

  // Here we are duplicating some tests here to verify whether no table entity is created
  // for hive table but process has reference for hive table.

  // borrowed from LoadDataHarvesterSuite
  test("LOAD DATA [LOCAL] INPATH path source") {
    val file = Files.createTempFile("input", ".txt").toFile
    val out = new PrintWriter(new FileOutputStream(file))
    out.write("a\nb\nc\nd\n")
    out.close()

    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    sparkSession.sql(s"LOAD DATA LOCAL INPATH '${file.getAbsolutePath}' " +
      s"OVERWRITE INTO TABLE $dbName.$sourceTblName").queryExecution

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val input = getOnlyOneEntity(entities, external.FS_PATH_TYPE_STRING)
    val expectedInputs = AtlasUtils.entitiesToReferences(Seq(input), useGuid = false)

    val expectedOutputs = Set(
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, sourceTblName)))

    validateProcessEntityWithAtlasEntities(entities, _ => {}, expectedInputs, expectedOutputs)
  }

  // borrowed from InsertIntoHiveDirHarvesterSuite
  test("INSERT OVERWRITE DIRECTORY path...") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    sparkSession.sql(s"INSERT OVERWRITE DIRECTORY 'target/dir1' " +
      s"SELECT * FROM $dbName.$sourceTblName").queryExecution

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val expectedInputs = Set(
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, sourceTblName)))

    val output = getOnlyOneEntity(entities, external.FS_PATH_TYPE_STRING)
    val dir = new File("target/dir1").getAbsolutePath
    assertFsEntity(output, dir)
    val expectedOutputs = AtlasUtils.entitiesToReferences(Seq(output), useGuid = false)

    validateProcessEntityWithAtlasEntities(entities, _ => {}, expectedInputs, expectedOutputs)
  }

}
