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

import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.{AtlasClientConf, TestUtils}
import com.hortonworks.spark.atlas.utils.SparkUtils

class SparkAtlasEntityUtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import TestUtils._

  private var sparkSession: SparkSession = _

  private var sparkAtlasEntityUtils: AtlasEntityUtils = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    sparkAtlasEntityUtils = new AtlasEntityUtils {
      override def conf: AtlasClientConf = new AtlasClientConf
    }
  }

  override protected def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null
    sparkAtlasEntityUtils = null
    super.afterAll()
  }

  test("convert catalog db to entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val dbEntities = sparkAtlasEntityUtils.dbToEntities(dbDefinition)

    val dbEntity = dbEntities.head
    dbEntity.getTypeName should be (metadata.DB_TYPE_STRING)
    dbEntity.getAttribute("name") should be ("db1")
    dbEntity.getAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE) should be (
      AtlasConstants.DEFAULT_CLUSTER_NAME)
    dbEntity.getAttribute("location") should be ("hdfs:///test/db/db1")
    dbEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1")
  }

  test("convert catalog storage format to entity") {
    val storageFormat = createStorageFormat()
    val sdEntities =
      sparkAtlasEntityUtils.storageFormatToEntities(storageFormat, "db1", "tbl1", false)

    val sdEntity = sdEntities.head
    sdEntity.getTypeName should be (metadata.STORAGEDESC_TYPE_STRING)
    sdEntity.getAttribute("location") should be (null)
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

    val schemaEntities = sparkAtlasEntityUtils.schemaToEntities(schema, "db1", "tbl1", false)
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

    val tableEntities = sparkAtlasEntityUtils.tableToEntities(tableDefinition, Some(dbDefinition))
    val tableEntity = tableEntities.head

    val dbEntity = tableEntities.find(_.getTypeName == metadata.DB_TYPE_STRING).get
    val sdEntity = tableEntities.find(_.getTypeName == metadata.STORAGEDESC_TYPE_STRING).get
    val schemaEntities = tableEntities.filter(_.getTypeName == metadata.COLUMN_TYPE_STRING)

    tableEntity.getTypeName should be (metadata.TABLE_TYPE_STRING)
    tableEntity.getAttribute("name") should be ("tbl1")
    tableEntity.getAttribute("db") should be (dbEntity)
    tableEntity.getAttribute("owner") should be (SparkUtils.currUser())
    tableEntity.getAttribute("ownerType") should be ("USER")
    tableEntity.getAttribute("sd") should be (sdEntity)
    tableEntity.getAttribute("columns") should be (schemaEntities.asJava)
  }
}

