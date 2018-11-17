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
import java.util.Locale

import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.AtlasClientConf
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.streaming.StreamTest

class SparkExecutionPlanProcessorForBatchQuerySuite extends StreamTest {
  import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  override def beforeAll(): Unit = {
    super.beforeAll()
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    testHelperQueryListener.clear()
    spark.listenerManager.register(testHelperQueryListener)
  }

  override def afterAll(): Unit = {
    atlasClient = null
    spark.listenerManager.unregister(testHelperQueryListener)
    super.afterAll()
  }

  test("Read csv file and save as table") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val csvContent = Seq("a,1", "b,2", "c,3", "d,4").mkString("\n")
    val tempFile: Path = writeCSVtextToTempFile(csvContent)

    val rand = new scala.util.Random()
    val outputTableName = "app_details_" + rand.nextInt(1000000000)

    val df = spark.read.csv(tempFile.toAbsolutePath.toString)
    df.write.saveAsTable(outputTableName)

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)

    // only assert qualifiedName and skip assertion on database, and attributes on database
    // they should be covered in other UT
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    assert(tableQualifiedName.endsWith(outputTableName))

    // remove table name + '.' prior to table name
    val databaseQualifiedName = tableQualifiedName.substring(0,
      tableQualifiedName.indexOf(outputTableName) - 1)

    val databaseEntity = getOnlyOneEntity(entities, metadata.DB_TYPE_STRING)
    assert(getStringAttribute(databaseEntity, "qualifiedName") === databaseQualifiedName)

    // database entity in table entity should be same as outer database entity
    val databaseEntityInTable = getAtlasEntityAttribute(tableEntity, "db")
    assert(databaseEntity === databaseEntityInTable)

    val databaseLocationFsEntity = getAtlasEntityAttribute(databaseEntity, "locationUri")

    // we're expecting two file system entities:
    // one for input file, another one for database warehouse
    val fsEntities = listAtlasEntitiesAsType(entities, external.FS_PATH_TYPE_STRING)
    assert(fsEntities.size === 2)

    // database warehouse
    assert(fsEntities.contains(databaseLocationFsEntity))

    val inputFsEntities = fsEntities.filterNot(_ == databaseLocationFsEntity)
    assert(inputFsEntities.size === 1)

    // input file
    val inputFsEntity = inputFsEntities.head

    assertPathsEquals(getStringAttribute(inputFsEntity, "name"), tempFile.toAbsolutePath.toString)
    assertPathsEquals(getStringAttribute(inputFsEntity, "path"), tempFile.toAbsolutePath.toString)
    assertPathsEquals(getStringAttribute(inputFsEntity, "qualifiedName"),
      "file://" + tempFile.toAbsolutePath.toString)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    val input = getOnlyOneEntity(inputs, external.FS_PATH_TYPE_STRING)
    val output = getOnlyOneEntity(outputs, metadata.TABLE_TYPE_STRING)

    // input/output in 'spark_process' should be same as outer entities
    assert(input === inputFsEntity)
    assert(output === tableEntity)

    val expectedMap = Map(
      "executionId" -> queryDetail.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(queryDetail.qe),
      "executionTime" -> queryDetail.executionTime.toString,
      "details" -> queryDetail.qe.toString()
    )

    expectedMap.foreach { case (key, value) =>
      assert(processEntity.getAttribute(key) === value)
    }
  }

  private def writeCSVtextToTempFile(csvContent: String) = {
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

  private def assertPathsEquals(path1: String, path2: String): Unit = {
    // we are comparing paths with lower case, since we may want to run the test
    // from case-insensitive filesystem
    assert(path1.toLowerCase(Locale.ROOT) === path2.toLowerCase(Locale.ROOT))
  }
}