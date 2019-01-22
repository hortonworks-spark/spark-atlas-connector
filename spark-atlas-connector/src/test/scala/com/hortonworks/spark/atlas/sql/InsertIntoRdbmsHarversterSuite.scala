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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Path}

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import java.sql.DriverManager

import com.hortonworks.spark.atlas.{AtlasClientConf, WithHiveSupport}
import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}
import com.hortonworks.spark.atlas.types.{external, metadata}
import org.apache.atlas.model.instance.AtlasEntity

class InsertIntoRdbmsHarversterSuite extends FunSuite with Matchers
  with BeforeAndAfter with WithHiveSupport {

  val tableName = "test_table"
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

    val DDL = s"CREATE TABLE $tableName (NAME VARCHAR(20))"
    val statement = connection.createStatement
    statement.executeUpdate(DDL)

    // setup Atlas client
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    sparkSession.listenerManager.register(testHelperQueryListener)
  }

  test("insert file into derby database") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val csvContent = Seq("A", "B", "C").mkString("\n")
    val tempFile: Path = writeCsvTextToTempFile(csvContent)

    // insert tempFile into derby database
    val jdbcProperties = new java.util.Properties
    jdbcProperties.setProperty("driver", jdbcDriver)
    val url = s"jdbc:derby:memory:$databaseName;create=false"
    val df = sparkSession.read.csv(tempFile.toAbsolutePath.toString).toDF("Name")
    df.write.mode("append").jdbc(url, tableName, jdbcProperties)

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    // we're expecting one file system entities,for input file
    val fsEntities = listAtlasEntitiesAsType(entities, external.FS_PATH_TYPE_STRING)
    assert(fsEntities.size === 1)

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, "rdbms_table")
    assertTableEntity(tableEntity, databaseName + "." + tableName)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")
    val input = getOnlyOneEntity(inputs, external.FS_PATH_TYPE_STRING)
    val output = getOnlyOneEntity(outputs, "rdbms_table")

    // input/output in 'spark_process' should be same as outer entities
    assert(input === fsEntities.head)
    assert(output === tableEntity)
  }

  private def assertTableEntity(tableEntity: AtlasEntity, tableName: String): Unit = {
    // only assert qualifiedName and skip assertion on database, and attributes on database
    // they should be covered in other UT
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    assert(tableQualifiedName.endsWith(tableName))
  }

  private def writeCsvTextToTempFile(csvContent: String) = {
    val tempFile = Files.createTempFile("spark-atlas-connector-csv-temp", ".csv")

    // remove temporary file in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          Files.deleteIfExists(tempFile)
        }
      }, 10)

    val bw = new BufferedWriter(new FileWriter(tempFile.toFile))
    bw.write(csvContent)
    bw.close()
    tempFile
  }

}
