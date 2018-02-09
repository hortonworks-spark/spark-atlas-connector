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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.atlas.AtlasServiceException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.{BaseResourceIT, RestAtlasClient, TestUtils}

class CatalogEventToAtlasIT extends BaseResourceIT with Matchers {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var tracker: SparkCatalogEventTracker = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
    tracker = new SparkCatalogEventTracker(new RestAtlasClient(atlasClientConf), atlasClientConf)
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.afterAll()
  }

  it("catalog db event to Atlas entities") {
    // Create db entity in Atlas and make sure we get it from Atlas
    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(tracker.dbType, tracker.dbUniqueAttribute("db1"))
      entity should not be (null)
      entity.getAttribute("name") should be ("db1")
    }

    // Drop DB from external catalog to make sure we also delete the corresponding Atlas entity
    SparkUtils.getExternalCatalog().dropDatabase("db1", ignoreIfNotExists = true, cascade = false)
    tracker.onOtherEvent(DropDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      intercept[AtlasServiceException](
        getEntity(tracker.dbType, tracker.dbUniqueAttribute("db1")))
    }
  }

  it("catalog table event to Atlas entities") {
    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db2", tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateDatabaseEvent("db2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(tracker.dbType, tracker.dbUniqueAttribute("db2"))
      entity should not be (null)
      entity.getAttribute("name") should be ("db2")
    }

    // Create new table
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)
    val sd = CatalogStorageFormat.empty
    val tableDefinition = createTable("db2", "tbl1", schema, sd)
    val isHiveTbl = tracker.isHiveTable(tableDefinition)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateTableEvent("db2", "tbl1"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val sdEntity = getEntity(tracker.storageFormatType(isHiveTbl),
        tracker.storageFormatUniqueAttribute("db2", "tbl1", isHiveTbl))
      sdEntity should not be (null)

      schema.foreach { s =>
        val colEntity = getEntity(tracker.columnType(isHiveTbl),
          tracker.columnUniqueAttribute("db2", "tbl1", s.name, isHiveTbl))
        colEntity should not be (null)
        colEntity.getAttribute("name") should be (s.name)
        colEntity.getAttribute("type") should be (s.dataType.typeName)
      }

      val tblEntity = getEntity(tracker.tableType(isHiveTbl),
        tracker.tableUniqueAttribute("db2", "tbl1", isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be ("tbl1")
    }

    // Rename table
    SparkUtils.getExternalCatalog().renameTable("db2", "tbl1", "tbl2")
    tracker.onOtherEvent(RenameTableEvent("db2", "tbl1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val tblEntity = getEntity(tracker.tableType(isHiveTbl),
        tracker.tableUniqueAttribute("db2", "tbl2", isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be ("tbl2")

      schema.foreach { s =>
        val colEntity = getEntity(tracker.columnType(isHiveTbl),
          tracker.columnUniqueAttribute("db2", "tbl2", s.name, isHiveTbl))
        colEntity should not be (null)
        colEntity.getAttribute("name") should be (s.name)
        colEntity.getAttribute("type") should be (s.dataType.typeName)
      }

      val sdEntity = getEntity(tracker.storageFormatType(isHiveTbl),
        tracker.storageFormatUniqueAttribute("db2", "tbl2", isHiveTbl))
      sdEntity should not be (null)
    }

    // Drop table
    val tblDef2 = SparkUtils.getExternalCatalog().getTable("db2", "tbl2")
    val isHiveTbl2 = tracker.isHiveTable(tblDef2)
    tracker.onOtherEvent(DropTablePreEvent("db2", "tbl2"))
    tracker.onOtherEvent(DropTableEvent("db2", "tbl2"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      intercept[AtlasServiceException](getEntity(tracker.tableType(isHiveTbl),
        tracker.tableUniqueAttribute("db2", "tbl2", isHiveTbl2)))
    }
  }
}
