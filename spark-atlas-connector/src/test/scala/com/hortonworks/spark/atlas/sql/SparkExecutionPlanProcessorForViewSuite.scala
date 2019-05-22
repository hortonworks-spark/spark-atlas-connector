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

import scala.util.Random
import org.scalatest.{FunSuite, Matchers}
import org.apache.atlas.model.instance.AtlasEntity
import com.hortonworks.spark.atlas.AtlasEntityReadHelper._
import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, WithHiveSupport}
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor, ProcessEntityValidator}
import com.hortonworks.spark.atlas.types.metadata

class SparkExecutionPlanProcessorForViewSuite
  extends FunSuite
  with Matchers
  with WithHiveSupport
  with ProcessEntityValidator {
  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val destinationViewName = "destination_" + Random.nextInt(100000)
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

  test("CREATE TEMPORARY VIEW FROM TABLE, SAVE TEMP VIEW TO TABLE") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)
    sparkSession.sql(s"SELECT * FROM $sourceTblName").createOrReplaceTempView(destinationViewName)

    var queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    var entities = atlasClient.createdEntities

    // no entities should have been received from a creating a temporary view
    assert(entities.isEmpty)

    // we don't want to check above queries, so reset the entities in listener
    testHelperQueryListener.clear()

    sparkSession.sql(s"SELECT * FROM $destinationViewName").write.saveAsTable(destinationTableName)

    queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    entities = atlasClient.createdEntities

    // we're expecting two table entities:
    // one from the source table and another from the sink table, the temporary view is ignored
    assert(entities.nonEmpty)
    val tableEntities = listAtlasEntitiesAsType(entities, metadata.TABLE_TYPE_STRING)
    assert(tableEntities.size === 2)

    val inputEntity = getOnlyOneEntityOnAttribute(tableEntities, "name", sourceTblName)
    val outputEntity = getOnlyOneEntityOnAttribute(tableEntities, "name", destinationTableName)
    assertTableEntity(inputEntity, sourceTblName)
    assertTableEntity(outputEntity, destinationTableName)

    // check for 'spark_process'
    validateProcessEntityWithAtlasEntities(entities, _ => {},
      AtlasUtils.entitiesToReferences(Seq(inputEntity)),
      AtlasUtils.entitiesToReferences(Seq(outputEntity)))
  }

  private def assertTableEntity(entity: AtlasEntity, tableName: String): Unit = {
    val tableQualifiedName = getStringAttribute(entity, "qualifiedName")
    assert(tableQualifiedName.contains(s"$tableName"))
  }
}
