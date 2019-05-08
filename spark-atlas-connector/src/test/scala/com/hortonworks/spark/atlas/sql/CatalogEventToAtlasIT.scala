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

  private var processor: SparkCatalogEventProcessor = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()
    processor =
      new SparkCatalogEventProcessor(new RestAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    super.afterAll()
  }

  it("catalog db event to Atlas entities") {
    val dbName = uniqueName("db1")

    // Create db entity in Atlas and make sure we get it from Atlas
    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB(dbName, tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(processor.dbType, processor.dbUniqueAttribute(dbName))
      entity should not be (null)
      entity.getAttribute("name") should be (dbName)
      entity.getAttribute("owner") should be (SparkUtils.currUser())
      entity.getAttribute("ownerType") should be ("USER")
    }

    // Drop DB from external catalog to make sure we also delete the corresponding Atlas entity
    SparkUtils.getExternalCatalog().dropDatabase(dbName, ignoreIfNotExists = true, cascade = false)
    processor.pushEvent(DropDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      intercept[AtlasServiceException](
        getEntity(processor.dbType, processor.dbUniqueAttribute(dbName)))
    }
  }

  it("catalog table event to Atlas entities") {
    val dbName = uniqueName("db2")
    val tbl1Name = uniqueName("tbl1")
    val tbl2Name = uniqueName("tbl2")

    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB(dbName, tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateDatabaseEvent(dbName))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(processor.dbType, processor.dbUniqueAttribute(dbName))
      entity should not be (null)
      entity.getAttribute("name") should be (dbName)
    }

    // Create new table
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)
    val sd = CatalogStorageFormat.empty
    val tableDefinition = createTable(dbName, tbl1Name, schema, sd)
    val isHiveTbl = processor.isHiveTable(tableDefinition)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateTableEvent(dbName, tbl1Name))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val sdEntity = getEntity(processor.storageFormatType(isHiveTbl),
        processor.storageFormatUniqueAttribute(dbName, tbl1Name, isHiveTbl))
      sdEntity should not be (null)

      val tblEntity = getEntity(processor.tableType(isHiveTbl),
        processor.tableUniqueAttribute(dbName, tbl1Name, isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl1Name)
    }

    // Rename table
    SparkUtils.getExternalCatalog().renameTable(dbName, tbl1Name, tbl2Name)
    processor.pushEvent(RenameTableEvent(dbName, tbl1Name, tbl2Name))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val tblEntity = getEntity(processor.tableType(isHiveTbl),
        processor.tableUniqueAttribute(dbName, tbl2Name, isHiveTbl))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be (tbl2Name)

      val sdEntity = getEntity(processor.storageFormatType(isHiveTbl),
        processor.storageFormatUniqueAttribute(dbName, tbl2Name, isHiveTbl))
      sdEntity should not be (null)
    }

    // Drop table
    val tblDef2 = SparkUtils.getExternalCatalog().getTable(dbName, tbl2Name)
    val isHiveTbl2 = processor.isHiveTable(tblDef2)
    processor.pushEvent(DropTablePreEvent(dbName, tbl2Name))
    processor.pushEvent(DropTableEvent(dbName, tbl2Name))

    // sleeping 2 secs - we have to do this to ensure there's no call on deletion, unfortunately...
    Thread.sleep(2 * 1000)
    // deletion request should not be added
    val tblEntity = getEntity(processor.tableType(isHiveTbl),
      processor.tableUniqueAttribute(dbName, tbl2Name, isHiveTbl2))
    tblEntity should not be (null)
  }
}
