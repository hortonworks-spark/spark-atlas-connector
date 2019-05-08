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

package com.hortonworks.spark.atlas

import java.nio.file.Files

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.atlas.AtlasServiceException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.Matchers

import com.hortonworks.spark.atlas.utils.SparkUtils

class KafkaClientIT extends BaseResourceIT with Matchers {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var tracker: SparkAtlasEventTracker = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    tracker = new SparkAtlasEventTracker(new KafkaAtlasClient(atlasClientConf), atlasClientConf)
  }

  override protected def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    super.afterAll()
  }

  it("create / update / delete new entities") {
    val dbName = uniqueName("db2")
    val tbl1Name = uniqueName("tbl1")
    val tbl3Name = uniqueName("tbl3")

    // Create new DB
    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB(dbName, tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(
        tracker.catalogEventTracker.dbType, tracker.catalogEventTracker.dbUniqueAttribute(dbName))
      entity should not be (null)
      entity.getAttribute("name") should be (dbName)
    }

    // Create new table
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)
    val sd = CatalogStorageFormat.empty
    val tableDefinition = createTable(dbName, tbl1Name, schema, sd)
    val isHiveTbl = tracker.catalogEventTracker.isHiveTable(tableDefinition)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateTableEvent(dbName, tbl1Name))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val sdEntity = getEntity(tracker.catalogEventTracker.storageFormatType(isHiveTbl),
        tracker.catalogEventTracker.storageFormatUniqueAttribute(dbName, tbl1Name, isHiveTbl))
      sdEntity should not be (null)

      val tblEntity = getEntity(tracker.catalogEventTracker.tableType(isHiveTbl),
        tracker.catalogEventTracker.tableUniqueAttribute(dbName, tbl1Name, isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl1Name)
    }

    // Rename table
    SparkUtils.getExternalCatalog().renameTable(dbName, tbl1Name, tbl3Name)
    tracker.onOtherEvent(RenameTableEvent(dbName, tbl1Name, tbl3Name))
    val newTblDef = SparkUtils.getExternalCatalog().getTable(dbName, tbl3Name)
    val isNewTblHive = tracker.catalogEventTracker.isHiveTable(newTblDef)

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val tblEntity = getEntity(tracker.catalogEventTracker.tableType(isHiveTbl),
        tracker.catalogEventTracker.tableUniqueAttribute(dbName, tbl3Name, isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl3Name)

      val sdEntity = getEntity(tracker.catalogEventTracker.storageFormatType(isHiveTbl),
        tracker.catalogEventTracker.storageFormatUniqueAttribute(dbName, tbl3Name, isHiveTbl))
      sdEntity should not be (null)
    }

    // Drop table
    tracker.onOtherEvent(DropTablePreEvent(dbName, tbl3Name))
    tracker.onOtherEvent(DropTableEvent(dbName, tbl3Name))

    // sleeping 2 secs - we have to do this to ensure there's no call on deletion, unfortunately...
    Thread.sleep(2 * 1000)
    // deletion request should not be added
    val tblEntity = getEntity(tracker.catalogEventTracker.tableType(isHiveTbl),
      tracker.catalogEventTracker.tableUniqueAttribute(dbName, tbl3Name, isNewTblHive))
    tblEntity should not be (null)
  }

}
