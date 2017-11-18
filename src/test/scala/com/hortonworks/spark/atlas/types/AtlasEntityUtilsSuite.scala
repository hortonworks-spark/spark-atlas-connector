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

import scala.collection.JavaConverters._

import org.apache.atlas.AtlasClient
import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.TestUtils

class AtlasEntityUtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _

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

  test("convert catalog db to entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)

    dbEntity.getTypeName should be (metadata.DB_TYPE_STRING)
    dbEntity.getAttribute("name") should be ("db1")
    dbEntity.getAttribute("locationUri") should be ("hdfs:///test/db/db1")
    dbEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1")
  }

  test("convert catalog storage format to entity") {
    val storageFormat = createStorageFormat()
    val sdEntity = AtlasEntityUtils.storageFormatToEntity(storageFormat, "db1", "tbl1")

    sdEntity.getTypeName should be (metadata.STORAGEDESC_TYPE_STRING)
    sdEntity.getAttribute("locationUri") should be (null)
    sdEntity.getAttribute("inputFormat") should be (null)
    sdEntity.getAttribute("outputFormat") should be (null)
    sdEntity.getAttribute("serde") should be (null)
    sdEntity.getAttribute("compressed") should be (java.lang.Boolean.FALSE)
    sdEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1.tbl1.storageFormat")
  }

  test("convert schema to entity") {
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)

    val schemaEntities = AtlasEntityUtils.schemaToEntity(schema, "db1", "tbl1")
    schemaEntities.length should be (2)

    schemaEntities(0).getAttribute("name") should be ("user")
    schemaEntities(0).getAttribute("type") should be ("string")
    schemaEntities(0).getAttribute("nullable") should be (java.lang.Boolean.FALSE)
    schemaEntities(0).getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1.tbl1.col-user")

    schemaEntities(1).getAttribute("name") should be ("age")
    schemaEntities(1).getAttribute("type") should be ("integer")
    schemaEntities(1).getAttribute("nullable") should be (java.lang.Boolean.TRUE)
    schemaEntities(1).getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1.tbl1.col-age")
  }

  test("convert table to entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", "tbl1", schema, sd)

    val dbEntity = AtlasEntityUtils.dbToEntity(dbDefinition)
    val sdEntity = AtlasEntityUtils.storageFormatToEntity(sd, "db1", "tbl1")
    val schemaEntities = AtlasEntityUtils.schemaToEntity(schema, "db1", "tbl1")
    val tableEntity =
      AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity, schemaEntities, sdEntity)

    tableEntity.getTypeName should be (metadata.TABLE_TYPE_STRING)
    tableEntity.getAttribute("name") should be ("tbl1")
    tableEntity.getAttribute("database") should be (dbEntity)
    tableEntity.getAttribute("storage") should be (sdEntity)
    tableEntity.getAttribute("schema") should be (schemaEntities.asJava)
  }
}

