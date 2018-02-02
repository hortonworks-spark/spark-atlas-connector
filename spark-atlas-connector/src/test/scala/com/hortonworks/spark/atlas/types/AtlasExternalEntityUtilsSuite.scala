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

package com.hortonworks.spark.atlas.types

import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.atlas.AtlasClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.{AtlasClientConf, TestUtils}

class AtlasExternalEntityUtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var hiveAtlasEntityUtils: AtlasEntityUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    hiveAtlasEntityUtils = new AtlasEntityUtils {
      override def conf: AtlasClientConf = new AtlasClientConf
    }
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
    hiveAtlasEntityUtils = null
    super.afterAll()
  }

  test("convert catalog db to hive entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val dbEntities = hiveAtlasEntityUtils.dbToEntities(dbDefinition)

    val dbEntity = dbEntities.head
    dbEntity.getTypeName should be (external.HIVE_DB_TYPE_STRING)
    dbEntity.getAttribute("name") should be ("db1")
    dbEntity.getAttribute("location") should be (dbDefinition.locationUri.toString)
    dbEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be ("db1@primary")
  }

  test("convert catalog storage format to hive entity") {
    val storageFormat = createStorageFormat()
    val sdEntities =
      hiveAtlasEntityUtils.storageFormatToEntities(storageFormat, "db1", "tbl1", true)

    val sdEntity = sdEntities.head
    sdEntity.getTypeName should be (external.HIVE_STORAGEDESC_TYPE_STRING)
    sdEntity.getAttribute("location") should be (null)
    sdEntity.getAttribute("inputFormat") should be (null)
    sdEntity.getAttribute("outputFormat") should be (null)
    sdEntity.getAttribute("name") should be (null)
    sdEntity.getAttribute("compressed") should be (java.lang.Boolean.FALSE)
    sdEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1@primary_storage")
  }

  test("convert schema to hive entity") {
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)

    val schemaEntities = hiveAtlasEntityUtils.schemaToEntities(schema, "db1", "tbl1", true)
    schemaEntities.length should be (2)

    schemaEntities(0).getAttribute("name") should be ("user")
    schemaEntities(0).getAttribute("type") should be ("string")
    schemaEntities(0).getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1.user@primary")

    schemaEntities(1).getAttribute("name") should be ("age")
    schemaEntities(1).getAttribute("type") should be ("integer")
    schemaEntities(1).getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1.age@primary")
  }

  test("convert table to hive entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", "tbl1", schema, sd, true)

    val tableEntities = hiveAtlasEntityUtils.tableToEntities(tableDefinition, Some(dbDefinition))
    val tableEntity = tableEntities.head

    val dbEntity = tableEntities.find(_.getTypeName == external.HIVE_DB_TYPE_STRING).get
    val sdEntity = tableEntities.find(_.getTypeName == external.HIVE_STORAGEDESC_TYPE_STRING).get
    val schemaEntities = tableEntities.filter(_.getTypeName == external.HIVE_COLUMN_TYPE_STRING)

    tableEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    tableEntity.getAttribute("name") should be ("tbl1")
    tableEntity.getAttribute("db") should be (dbEntity)
    tableEntity.getAttribute("sd") should be (sdEntity)
    tableEntity.getAttribute("columns") should be (schemaEntities.asJava)
    tableEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1@primary")
  }

  test("convert path to entity") {
    val tempFile = Files.createTempFile("tmp", ".txt").toFile
    val pathEntity = external.pathToEntity(tempFile.getAbsolutePath)

    pathEntity.getTypeName should be (external.FS_PATH_TYPE_STRING)
    pathEntity.getAttribute("name") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntity.getAttribute("path") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      tempFile.toURI.toString)
  }
}

