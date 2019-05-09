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

import java.nio.file.Files

import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, WithHiveSupport}
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}
import com.hortonworks.spark.atlas.types.metadata
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class SparkExecutionPlanProcessorForHiveMetastoreTableSuite
  extends FunSuite with Matchers with BeforeAndAfterEach with WithHiveSupport {
  import com.hortonworks.spark.atlas.AtlasEntityReadHelper._

  val brokerProps: Map[String, Object] = Map[String, Object]()
  var kafkaTestUtils: KafkaTestUtils = _

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  val clusterName: String = atlasClientConf.get(AtlasClientConf.CLUSTER_NAME)

  override def beforeAll(): Unit = {
    super.beforeAll()
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

    sparkSession.sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS $outputTableName " +
      "(name STRING, age INT, emp_id INT, designation STRING) " +
      s"LOCATION '$tempDir'")

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)
    assertTableEntity(tableEntity, outputTableName)

    // database
    val databaseEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.DB_TYPE_STRING)
    assertDatabaseEntity(databaseEntity, tableEntity, outputTableName)

    // storage description
    val storageEntity = getOnlyOneEntity(entities, metadata.STORAGEDESC_TYPE_STRING)
    assertStorageDefinitionEntity(storageEntity, tableEntity)

    val databaseLocationString = getStringAttribute(databaseEntity, "location")
    assert(databaseLocationString != null && databaseLocationString.nonEmpty)
    assert(databaseLocationString.contains("sac-warehouse-"))
  }

  private def assertTableEntity(tableEntity: AtlasEntity, tableName: String): Unit = {
    // only assert qualifiedName and skip assertion on database, and attributes on database
    // they should be covered in other UT
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    // cluster name
    assert(tableQualifiedName.endsWith(s"$tableName@$clusterName"))
    // Other columns are also verified in CreateHiveTableAsSelectHarvesterSuite,
    // so we skip verifying here.
  }

  private def assertDatabaseEntity(
      databaseEntity: AtlasEntity,
      tableEntity: AtlasEntity,
      tableName: String): Unit = {
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    // remove table name + '.' prior to table name
    val databaseQualifiedName = tableQualifiedName.substring(0,
      tableQualifiedName.indexOf(tableName) - 1) + s"@$clusterName"

    assert(getStringAttribute(databaseEntity, "qualifiedName") === databaseQualifiedName)

    // database entity in table entity should be same as outer database entity
    val databaseEntityInTable = getAtlasObjectIdAttribute(tableEntity, "db")
    assert(AtlasUtils.entityToReference(databaseEntity) === databaseEntityInTable)

    val databaseLocationString = getStringAttribute(databaseEntity, "location")
    assert(databaseLocationString != null && databaseLocationString.nonEmpty)
    assert(databaseLocationString.contains("sac-warehouse-"))
  }

  private def assertStorageDefinitionEntity(
      sdEntity: AtlasEntity,
      tableEntity: AtlasEntity): Unit = {
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    val storageQualifiedName = tableQualifiedName + "_storage"
    assert(getStringAttribute(sdEntity, "qualifiedName") === storageQualifiedName)

    val storageEntityInTableAttribute = getAtlasObjectIdAttribute(tableEntity, "sd")
    assert(AtlasUtils.entityToReference(sdEntity) === storageEntityInTableAttribute)
  }
}
