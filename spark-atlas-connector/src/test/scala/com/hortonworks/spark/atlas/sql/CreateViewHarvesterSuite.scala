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

import java.util

import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExecutedCommandExec}
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.{AtlasClientConf, WithHiveSupport}
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}


class CreateViewHarvesterSuite extends FunSuite with Matchers with WithHiveSupport {
  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val destinationViewName = "destination_" + Random.nextInt(100000)
  private val destinationViewName2 = "destination_" + Random.nextInt(100000)
  private val destinationViewName3 = "destination_" + Random.nextInt(100000)
  private val destinationTableName = "destination_" + Random.nextInt(100000)

  private val testHelperQueryListener = new AtlasQueryExecutionListener()

  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('lucy'), ('tom')")

    // setup Atlas client
    testHelperQueryListener.clear()
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    sparkSession.listenerManager.register(testHelperQueryListener)
  }

  test("CREATE VIEW FROM TABLE") {
    val qe = sparkSession.sql(s"CREATE VIEW $destinationViewName " +
      s"AS SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTbl.getAttribute("name") should be (sourceTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should be (
      s"default.$sourceTblName@primary")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputView = outputs.asScala.head
    outputView.getAttribute("name") should be (destinationViewName)
    outputView.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$destinationViewName")
  }

  test("CREATE VIEW without source") {
    val qe = sparkSession.sql(s"CREATE VIEW $destinationViewName2 " +
      s"AS SELECT 1").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    val pEntity = entities.head
    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    assert(pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]].size() == 0)

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputView = outputs.asScala.head
    outputView.getAttribute("name") should be (destinationViewName2)
    outputView.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$destinationViewName2")
  }

  test("CREATE TEMPORARY VIEW FROM TABLE, SAVE TEMP VIEW TO TABLE") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)
    sparkSession.sql(s"SELECT * FROM $sourceTblName").createOrReplaceTempView(destinationViewName3)

    var queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    var entities = atlasClient.createdEntities

    // no entities should have been received from a creating a temporary view
    assert(entities.isEmpty)

    // we don't want to check above queries, so reset the entities in listener
    testHelperQueryListener.clear()

    sparkSession.sql(s"SELECT * FROM $destinationViewName3").write.saveAsTable(destinationTableName)

    queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    entities = atlasClient.createdEntities

    // we're expecting two table entities:
    // one from the source table and another from the sink table, the temporary view is ignored
    assert(!entities.isEmpty)
    val tableEntities = listAtlasEntitiesAsType(entities, metadata.TABLE_TYPE_STRING)
    assert(tableEntities.size === 2)

    val inputEntity = getOneEntityOnAttribute(tableEntities, "name", sourceTblName)
    val outputEntity = getOneEntityOnAttribute(tableEntities, "name", destinationTableName)
    assertTableEntity(inputEntity, sourceTblName)
    assertTableEntity(outputEntity, destinationTableName)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)
    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")
    assert(inputs.size === 1)
    assert(outputs.size === 1)

    // input/output in 'spark_process' should be same as outer entities
    val input = getOnlyOneEntity(inputs, metadata.TABLE_TYPE_STRING)
    val output = getOnlyOneEntity(outputs, metadata.TABLE_TYPE_STRING)
    assert(input === inputEntity)
    assert(output === outputEntity)
  }

  private def assertTableEntity(entity: AtlasEntity, tableName: String): Unit = {
    val tableQualifiedName = getStringAttribute(entity, "qualifiedName")
    assert(tableQualifiedName.contains(s"$tableName"))
  }
}
