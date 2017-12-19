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

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{LongType, StructType}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, TestUtils}
import com.hortonworks.spark.atlas.utils.SparkUtils

class SparkCatalogEventTrackerSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _
  private val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
  }

  test("correctly handle DB related events") {
    val tracker =
      new SparkCatalogEventTracker(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      tracker.atlasClient should not be (null)
      atlasClient = tracker.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tempPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall.size > 0)
      assert(atlasClient.createEntityCall(tracker.dbType) == 1)
    }

    tracker.onOtherEvent(DropDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall.size > 0)
      assert(atlasClient.deleteEntityCall(tracker.dbType) == 1)
    }
  }

  test("correctly handle table related events") {
    val tracker =
      new SparkCatalogEventTracker(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      tracker.atlasClient should not be (null)
      atlasClient = tracker.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tableDefinition =
      createTable("db1", "tbl1", new StructType().add("id", LongType), CatalogStorageFormat.empty)
    val isHiveTbl = tracker.isHiveTable(tableDefinition)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    tracker.onOtherEvent(CreateTableEvent("db1", "tbl1"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall(tracker.dbType) == 1)
      assert(atlasClient.createEntityCall(tracker.tableType(isHiveTbl)) == 1)
      assert(atlasClient.createEntityCall(tracker.columnType(isHiveTbl)) == 1)
      assert(atlasClient.createEntityCall(tracker.storageFormatType(isHiveTbl)) == 1)
    }

    SparkUtils.getExternalCatalog().renameTable("db1", "tbl1", "tbl2")
    tracker.onOtherEvent(RenameTableEvent("db1", "tbl1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.updateEntityCall(tracker.storageFormatType(isHiveTbl)) == 1)
      assert(atlasClient.updateEntityCall(tracker.columnType(isHiveTbl)) == 1)
      assert(atlasClient.updateEntityCall(tracker.tableType(isHiveTbl)) == 1)
    }

    tracker.onOtherEvent(DropTablePreEvent("db1", "tbl2"))
    SparkUtils.getExternalCatalog().dropTable("db1", "tbl2", true, true)
    tracker.onOtherEvent(DropTableEvent("db1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall(tracker.tableType(isHiveTbl)) == 1)
    }
  }
}

class FirehoseAtlasClient(conf: AtlasClientConf) extends AtlasClient {
  var createEntityCall = new mutable.HashMap[String, Int]
  var updateEntityCall = new mutable.HashMap[String, Int]
  var deleteEntityCall = new mutable.HashMap[String, Int]

  var processedEntity: AtlasEntity = _

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    new AtlasTypesDef()
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    entities.foreach { e =>
      createEntityCall(e.getTypeName) =
        createEntityCall.getOrElseUpdate(e.getTypeName, 0) + 1
      processedEntity = e
    }
  }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    updateEntityCall(entityType) = updateEntityCall.getOrElse(entityType, 0) + 1
    processedEntity = entity
  }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = {
    deleteEntityCall(entityType) = deleteEntityCall.getOrElse(entityType, 0) + 1
  }
}

