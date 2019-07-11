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

import java.io.File

import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, WithRemoteHiveMetastoreServiceSupport}
import com.hortonworks.spark.atlas.sql.testhelper._
import com.hortonworks.spark.atlas.types.external
import org.apache.atlas.model.instance.AtlasObjectId
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class SparkExecutionPlanProcessorForComplicatedQuerySuite
  extends FunSuite
  with BeforeAndAfterEach
  with Matchers
  with WithRemoteHiveMetastoreServiceSupport
  with ProcessEntityValidator
  with TableEntityValidator
  with FsEntityValidator {

  import com.hortonworks.spark.atlas.AtlasEntityReadHelper._

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
  var testHelperQueryListener: AtlasQueryExecutionListener = _

  val clusterName: String = atlasClientConf.get(AtlasClientConf.CLUSTER_NAME)

  override def beforeAll(): Unit = {
    super.beforeAll()

    testHelperQueryListener = new AtlasQueryExecutionListener()
    sparkSession.listenerManager.register(testHelperQueryListener)
  }

  override def afterAll(): Unit = {
    sparkSession.listenerManager.unregister(testHelperQueryListener)
    super.afterAll()
  }

  test("select tbl1, tbl2 -> save to tbl3 -> select tbl3 -> save to file") {
    val atlasClient = new CreateEntitiesTrackingAtlasClient()
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val rand = new scala.util.Random()
    val randNum = rand.nextInt(1000000000)

    val table1 = s"t1_$randNum"
    val table2 = s"t2_$randNum"
    val table3 = s"t3_$randNum"
    val outputPath = s"/tmp/hdfs_$randNum"

    sparkSession.sql(s"create table ${dbName}.${table1}(col1 int)")
    sparkSession.sql(s"create table ${dbName}.${table2}(col2 int)")

    testHelperQueryListener.clear()

    sparkSession
      .sql(s"select * from ${dbName}.${table1}, ${dbName}.${table2} where col1=col2")
      .write
      .saveAsTable(s"${dbName}.${table3}")

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val expectedInputs = Set(
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, table1)),
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, table2)))

    val expectedOutputs = Set(
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, table3)))

    validateProcessEntityWithAtlasEntities(entities, _ => {}, expectedInputs, expectedOutputs)

    testHelperQueryListener.clear()
    atlasClient.clearEntities()

    sparkSession
      .sql(s"select * from ${dbName}.${table3}")
      .write
      .mode("append")
      .save(outputPath)

    val queryDetail2 = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail2)
    val entities2 = atlasClient.createdEntities

    val expectedInputs2 = Set(
      new AtlasObjectId(external.HIVE_TABLE_TYPE_STRING,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        external.hiveTableUniqueAttribute(clusterName, dbName, table3)))

    val output = getOnlyOneEntity(entities, external.FS_PATH_TYPE_STRING)
    val dir = new File(outputPath).getAbsolutePath
    assertFsEntity(output, dir)
    val expectedOutputs2 = AtlasUtils.entitiesToReferences(Seq(output), useGuid = false)

    validateProcessEntityWithAtlasEntities(entities2, _ => {}, expectedInputs2, expectedOutputs2)
  }
}
