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
import org.scalatest. Matchers

import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, metadata}
import com.hortonworks.spark.atlas.utils.SparkUtils

class KafkaClientIT extends BaseResourceIT with Matchers {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var tracker: SparkEntitiesTracker = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()

    atlasClientConf.set(AtlasClientConf.CLIENT_TYPE.key, "kafka")
    tracker = new SparkEntitiesTracker(atlasClientConf)
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    atlasClientConf.set(AtlasClientConf.CLIENT_TYPE.key, "kafka")

    super.afterAll()
  }

  it("create new entities") {
    // Create new DB
    val tempDbPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db2", tempDbPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateDatabaseEvent("db2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val entity = getEntity(metadata.DB_TYPE_STRING, AtlasEntityUtils.dbUniqueAttribute("db2"))
      entity should not be (null)
      entity.getAttribute("name") should be ("db2")
    }

    // Create new table
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)
    val sd = CatalogStorageFormat.empty
    val tableDefinition = createTable("db2", "tbl1", schema, sd)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateTableEvent("db2", "tbl1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val sdEntity = getEntity(metadata.STORAGEDESC_TYPE_STRING,
        AtlasEntityUtils.storageFormatUniqueAttribute("db2", "tbl1"))
      sdEntity should not be (null)

      schema.foreach { s =>
        val colEntity = getEntity(metadata.COLUMN_TYPE_STRING,
          AtlasEntityUtils.columnUniqueAttribute("db2", "tbl1", s.name))
        colEntity should not be (null)
        colEntity.getAttribute("name") should be (s.name)
        colEntity.getAttribute("type") should be (s.dataType.typeName)
      }

      val tblEntity = getEntity(metadata.TABLE_TYPE_STRING,
        AtlasEntityUtils.tableUniqueAttribute("db2", "tbl1"))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be ("tbl1")
    }
  }

  it("update entities") {
    val schema = new StructType()
      .add("user", StringType)
      .add("age", IntegerType)

    // Rename table
    SparkUtils.getExternalCatalog().renameTable("db2", "tbl1", "tbl3")
    tracker.onOtherEvent(RenameTableEvent("db2", "tbl1", "tbl3"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      val tblEntity = getEntity(metadata.TABLE_TYPE_STRING,
        AtlasEntityUtils.tableUniqueAttribute("db2", "tbl3"))
      tblEntity should not be (null)
      tblEntity.getAttribute("name") should be ("tbl3")

      schema.foreach { s =>
        val colEntity = getEntity(metadata.COLUMN_TYPE_STRING,
          AtlasEntityUtils.columnUniqueAttribute("db2", "tbl3", s.name))
        colEntity should not be (null)
        colEntity.getAttribute("name") should be (s.name)
        colEntity.getAttribute("type") should be (s.dataType.typeName)
      }

      val sdEntity = getEntity(metadata.STORAGEDESC_TYPE_STRING,
        AtlasEntityUtils.storageFormatUniqueAttribute("db2", "tbl3"))
      sdEntity should not be (null)
    }
  }

  it("remove entities") {
    // Drop table
    SparkUtils.getExternalCatalog().dropTable(
      "db2", "tbl2", ignoreIfNotExists = true, purge = false)
    tracker.onOtherEvent(DropTableEvent("db2", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      intercept[AtlasServiceException](getEntity(metadata.TABLE_TYPE_STRING,
        AtlasEntityUtils.tableUniqueAttribute("db2", "tbl2")))
    }
  }
}
