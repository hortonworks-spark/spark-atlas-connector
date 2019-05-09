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
import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, TestUtils}
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
    val dbEntity = sparkAtlasEntityUtils.dbToEntity(dbDefinition)

    dbEntity.entity.getTypeName should be (metadata.DB_TYPE_STRING)
    dbEntity.entity.getAttribute("name") should be ("db1")
    dbEntity.entity.getAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE) should be (
      AtlasConstants.DEFAULT_CLUSTER_NAME)
    dbEntity.entity.getAttribute("location") should be ("hdfs:///test/db/db1")
    dbEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1")

    dbEntity.dependencies.length should be (0)
  }

  test("convert catalog storage format to entity") {
    val storageFormat = createStorageFormat()
    val sdEntity =
      sparkAtlasEntityUtils.storageFormatToEntity(storageFormat, "db1", "tbl1", false)

    sdEntity.entity.getTypeName should be (metadata.STORAGEDESC_TYPE_STRING)
    sdEntity.entity.getAttribute("location") should be (null)
    sdEntity.entity.getAttribute("inputFormat") should be (null)
    sdEntity.entity.getAttribute("outputFormat") should be (null)
    sdEntity.entity.getAttribute("serde") should be (null)
    sdEntity.entity.getAttribute("compressed") should be (java.lang.Boolean.FALSE)
    sdEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId + ".db1.tbl1.storageFormat")

    sdEntity.dependencies.length should be (0)
  }

  test("convert table to entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", "tbl1", schema, sd)

    val tableEntity = sparkAtlasEntityUtils.tableToEntity(tableDefinition, Some(dbDefinition))
    val tableDeps = tableEntity.dependencies.map(_.entity)

    val dbEntity = tableDeps.find(_.getTypeName == metadata.DB_TYPE_STRING).get
    val sdEntity = tableDeps.find(_.getTypeName == metadata.STORAGEDESC_TYPE_STRING).get

    tableEntity.entity.getTypeName should be (metadata.TABLE_TYPE_STRING)
    tableEntity.entity.getAttribute("name") should be ("tbl1")
    tableEntity.entity.getAttribute("owner") should be (SparkUtils.currUser())
    tableEntity.entity.getAttribute("ownerType") should be ("USER")

    tableEntity.entity.getRelationshipAttribute("db") should be (
      AtlasUtils.entityToReference(dbEntity, useGuid = false))

    tableEntity.entity.getRelationshipAttribute("sd") should be (
      AtlasUtils.entityToReference(sdEntity, useGuid = false))
  }
}

