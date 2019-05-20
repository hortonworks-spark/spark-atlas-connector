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
import java.nio.file.Files

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.scalatest.concurrent.Eventually._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, TestUtils}
import com.hortonworks.spark.atlas.utils.SparkUtils

class SparkCatalogEventProcessorSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _
  private val atlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    sparkSession.sessionState.catalog.reset()
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null

    FileUtils.deleteDirectory(new File("spark-warehouse"))

    super.afterAll()
  }

  test("correctly handle spark DB related events") {
    val processor =
      new SparkCatalogEventProcessor(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()

    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      processor.atlasClient should not be (null)
      atlasClient = processor.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tempPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = false)
    processor.pushEvent(CreateDatabasePreEvent("db1"))
    processor.pushEvent(CreateDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall.size > 0)
      assert(atlasClient.createEntityCall(processor.sparkDbType) == 1)
    }

    // SAC-97: Spark delete the table before SAC receives the message.
    sparkSession.sessionState.catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = true)
    processor.pushEvent(DropDatabasePreEvent("db1"))
    processor.pushEvent(DropDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall.size > 0)
      assert(atlasClient.deleteEntityCall(processor.sparkDbType) == 1)
    }
  }

  test("correctly handle spark table related events") {
    val processor =
      new SparkCatalogEventProcessor(new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()

    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      processor.atlasClient should not be (null)
      atlasClient = processor.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tempPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = false)

    val tableDefinition =
      createTable("db1", "tbl1", new StructType().add("ID", LongType), CatalogStorageFormat.empty)
    SparkUtils.getExternalCatalog().createTable(tableDefinition, ignoreIfExists = true)
    processor.pushEvent(CreateTableEvent("db1", "tbl1"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall(processor.sparkDbType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkTableType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkStorageFormatType) == 1)
    }

    SparkUtils.getExternalCatalog().renameTable("db1", "tbl1", "tbl2")
    processor.pushEvent(RenameTableEvent("db1", "tbl1", "tbl2"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.updateEntityCall(processor.sparkStorageFormatType) == 1)
      assert(atlasClient.updateEntityCall(processor.sparkTableType) == 1)
    }

    val renamedTableDef = SparkUtils.getExternalCatalog().getTable("db1", "tbl2")

    val newSchema = renamedTableDef.schema.add("COL1", StringType)
    val newTableDefinition = renamedTableDef.copy(schema = newSchema)
    SparkUtils.getExternalCatalog().alterTable(newTableDefinition)
    processor.pushEvent(AlterTableEvent("db1", "tbl2", "table"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      // no creation on db type and storage format entities
      assert(atlasClient.createEntityCall(processor.sparkDbType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkStorageFormatType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkTableType) == 2)
    }

    processor.pushEvent(AlterTableEvent("db1", "tbl2", "dataSchema"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      // no creation on db type and storage format entities
      assert(atlasClient.createEntityCall(processor.sparkDbType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkStorageFormatType) == 1)
      assert(atlasClient.createEntityCall(processor.sparkTableType) == 2)
    }

    processor.pushEvent(DropTablePreEvent("db1", "tbl2"))
    processor.pushEvent(DropTableEvent("db1", "tbl2"))
    // sleeping 2 secs - we have to do this to ensure there's no call on deletion, unfortunately...
    Thread.sleep(2 * 1000)
    // deletion request should not be added from this event
    assert(atlasClient.deleteEntityCall.getOrElse(processor.sparkTableType, 0) == 0)

    val tableDefinition2 =
      createTable("db1", "tbl3", new StructType().add("ID", LongType), CatalogStorageFormat.empty)
    SparkUtils.getExternalCatalog().createTable(tableDefinition2, ignoreIfExists = true)
    processor.pushEvent(CreateTableEvent("db1", "tbl3"))

    // SAC-97: Spark delete the table before SAC receives the message.
    // We had to change the expectation: we can't delete entity
    // given we can't get any table information details in event.
    val t2 = TableIdentifier("tbl3", Some("db1"))
    sparkSession.sessionState.catalog.dropTable(t2, ignoreIfNotExists = false, purge = true)
    processor.pushEvent(DropTablePreEvent("db1", "tbl3"))
    processor.pushEvent(DropTableEvent("db1", "tbl3"))
    // sleeping 2 secs - we have to do this to ensure there's no call on deletion, unfortunately...
    Thread.sleep(2 * 1000)
    // deletion request should not be added from this event
    assert(atlasClient.deleteEntityCall.getOrElse(processor.sparkTableType, 0) == 0)
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

