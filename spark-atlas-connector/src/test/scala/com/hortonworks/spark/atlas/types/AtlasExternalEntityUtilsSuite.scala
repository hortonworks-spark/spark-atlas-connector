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
import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.{AtlasClientConf, TestUtils, WithHiveSupport}

class AtlasExternalEntityUtilsSuite extends FunSuite with Matchers with WithHiveSupport {
  import TestUtils._

  private var hiveAtlasEntityUtils: AtlasEntityUtils = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    hiveAtlasEntityUtils = new AtlasEntityUtils {
      override def conf: AtlasClientConf = new AtlasClientConf
    }
  }

  override def afterAll(): Unit = {
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

    tableEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    tableEntity.getAttribute("name") should be ("tbl1")
    tableEntity.getAttribute("db") should be (dbEntity)
    tableEntity.getAttribute("sd") should be (sdEntity)
    tableEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1@primary")
  }

  test("convert path to entity") {
    val tempFile = Files.createTempFile("tmp", ".txt").toFile
    val pathEntities = external.pathToEntities(tempFile.getAbsolutePath)

    pathEntities.head.getTypeName should be (external.FS_PATH_TYPE_STRING)
    pathEntities.head.getAttribute("name") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntities.head.getAttribute("path") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntities.head.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      tempFile.toURI.toString)
  }

  test("convert jdbc properties to rdbms entity") {
    val tableName = "employee"
    val rdbmsEntity = external.rdbmsTableToEntity("jdbc:mysql://localhost:3306/default", tableName)

    rdbmsEntity.head.getTypeName should be (external.RDBMS_TABLE)
    rdbmsEntity.head.getAttribute("name") should be (tableName)
    rdbmsEntity.head.getAttribute("qualifiedName") should be ("default." + tableName)
  }

  test("convert hbase properties to hbase table entity") {
    val cluster = "primary"
    val tableName = "employee"
    val nameSpace = "default"
    val hbaseEntity = external.hbaseTableToEntity(cluster, tableName, nameSpace)

    hbaseEntity.head.getTypeName should be (external.HBASE_TABLE_STRING)
    hbaseEntity.head.getAttribute("name") should be (tableName)
    hbaseEntity.head.getAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE) should be (cluster)
    hbaseEntity.head.getAttribute("uri") should be (
      nameSpace + ":" + tableName)
  }

  test("convert s3 path to aws_s3 entities") {
    val pathEntities = external.pathToEntities("s3://testbucket/testpseudodir/testfile")

    pathEntities.head.getTypeName should be (external.S3_OBJECT_TYPE_STRING)
    pathEntities.head.getAttribute("name") should be ("testfile")
    pathEntities.head.getAttribute("qualifiedName") should be (
      "s3://testbucket/testpseudodir/testfile")

    pathEntities.tail.head.getTypeName should be (external.S3_PSEUDO_DIR_TYPE_STRING)
    pathEntities.tail.head.getAttribute("name") should be ("/testpseudodir/")
    pathEntities.tail.head.getAttribute("qualifiedName") should be (
      "s3://testbucket/testpseudodir/")

    pathEntities.tail.tail.head.getTypeName should be (external.S3_BUCKET_TYPE_STRING)
    pathEntities.tail.tail.head.getAttribute("name") should be ("testbucket")
    pathEntities.tail.tail.head.getAttribute("qualifiedName") should be (
      "s3://testbucket")
  }

}

