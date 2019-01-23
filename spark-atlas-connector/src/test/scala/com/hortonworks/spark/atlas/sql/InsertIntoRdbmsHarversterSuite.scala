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

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import java.sql.DriverManager

import com.hortonworks.spark.atlas.{AtlasClientConf, WithHiveSupport}
import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}
import com.hortonworks.spark.atlas.types.{external, metadata}

class InsertIntoRdbmsHarversterSuite extends FunSuite with Matchers
  with BeforeAndAfter with WithHiveSupport {

  val sinkTableName = "sink_table"
  val sourceTableName = "source_table"
  val databaseName = "testdb"
  val jdbcDriver = "org.apache.derby.jdbc.EmbeddedDriver"

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  before {
    // setup derby database and necesaary table
    val connectionURL = s"jdbc:derby:memory:$databaseName;create=true"
    Class.forName(jdbcDriver)
    val connection = DriverManager.getConnection(connectionURL)

    val createSinkTableQuery = s"CREATE TABLE $sinkTableName (NAME VARCHAR(20))"
    val createSourceTableQuery = s"CREATE TABLE $sourceTableName (NAME VARCHAR(20))"
    val insertQuery = s"INSERT INTO $sourceTableName (Name) VALUES ('A'), ('B'), ('C')"
    val statement = connection.createStatement
    statement.executeUpdate(createSinkTableQuery)
    statement.executeUpdate(createSourceTableQuery)
    statement.executeUpdate(insertQuery)

    // setup Atlas client
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    sparkSession.listenerManager.register(testHelperQueryListener)
  }

  test("read from derby table and insert into a different derby table") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val jdbcProperties = new java.util.Properties
    jdbcProperties.setProperty("driver", jdbcDriver)
    val url = s"jdbc:derby:memory:$databaseName;create=false"

    val readDataFrame = sparkSession.read.jdbc(url, sourceTableName, jdbcProperties)
    readDataFrame.write.mode("append").jdbc(url, sinkTableName, jdbcProperties)

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    // we're expecting two table entities:
    // one from the source table and another from the sink table
    val tableEntities = listAtlasEntitiesAsType(entities, external.RDBMS_TABLE)
    assert(tableEntities.size === 2)

    val inputEntity = getOneEntityOnAttribute(tableEntities, "name", sourceTableName)
    val outputEntity = getOneEntityOnAttribute(tableEntities, "name", sinkTableName)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")
    assert(inputs.size === 1)
    assert(outputs.size === 1)

    // input/output in 'spark_process' should be same as outer entities
    val input = getOnlyOneEntity(inputs, external.RDBMS_TABLE)
    val output = getOnlyOneEntity(outputs, external.RDBMS_TABLE)
    assert(input === inputEntity)
    assert(output === outputEntity)
  }

}
