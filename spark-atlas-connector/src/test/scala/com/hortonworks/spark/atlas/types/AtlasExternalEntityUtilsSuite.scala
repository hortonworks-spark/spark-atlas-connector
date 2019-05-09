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
import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, TestUtils, WithHiveSupport}

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
    val dbEntity = hiveAtlasEntityUtils.dbToEntity(dbDefinition)

    dbEntity.entity.getTypeName should be (external.HIVE_DB_TYPE_STRING)
    dbEntity.entity.getAttribute("name") should be ("db1")
    dbEntity.entity.getAttribute("location") should be (dbDefinition.locationUri.toString)
    dbEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be ("db1@primary")

    dbEntity.dependencies.length should be (0)
  }

  test("convert catalog storage format to hive entity") {
    val storageFormat = createStorageFormat()
    val sdEntity =
      hiveAtlasEntityUtils.storageFormatToEntity(storageFormat, "db1", "tbl1", true)

    sdEntity.entity.getTypeName should be (external.HIVE_STORAGEDESC_TYPE_STRING)
    sdEntity.entity.getAttribute("location") should be (null)
    sdEntity.entity.getAttribute("inputFormat") should be (null)
    sdEntity.entity.getAttribute("outputFormat") should be (null)
    sdEntity.entity.getAttribute("name") should be (null)
    sdEntity.entity.getAttribute("compressed") should be (java.lang.Boolean.FALSE)
    sdEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1@primary_storage")

    sdEntity.dependencies.size should be (0)
  }

  test("convert table to hive entity") {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", "tbl1", schema, sd, true)

    val tableEntities = hiveAtlasEntityUtils.tableToEntity(tableDefinition, Some(dbDefinition))
    val tableEntity = tableEntities.entity
    val deps = tableEntities.dependencies.map(_.entity)

    val dbEntity = deps.find(_.getTypeName == external.HIVE_DB_TYPE_STRING).get
    val sdEntity = deps.find(_.getTypeName == external.HIVE_STORAGEDESC_TYPE_STRING).get

    tableEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    tableEntity.getAttribute("name") should be ("tbl1")
    tableEntity.getRelationshipAttribute("db") should be (
      AtlasUtils.entityToReference(dbEntity, useGuid = false))
    tableEntity.getRelationshipAttribute("sd") should be (
      AtlasUtils.entityToReference(sdEntity, useGuid = false))
    tableEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      "db1.tbl1@primary")
  }

  test("convert path to entity") {
    val tempFile = Files.createTempFile("tmp", ".txt").toFile
    val pathEntity = external.pathToEntity(tempFile.getAbsolutePath)

    pathEntity.entity.getTypeName should be (external.FS_PATH_TYPE_STRING)
    pathEntity.entity.getAttribute("name") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntity.entity.getAttribute("path") should be (tempFile.getAbsolutePath.toLowerCase)
    pathEntity.entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      tempFile.toURI.toString)

    pathEntity.dependencies.length should be (0)
  }

  test("convert jdbc properties to rdbms entity") {
    val tableName = "employee"
    val rdbmsEntity = external.rdbmsTableToEntity("jdbc:mysql://localhost:3306/default", tableName)

    rdbmsEntity.entity.getTypeName should be (external.RDBMS_TABLE)
    rdbmsEntity.entity.getAttribute("name") should be (tableName)
    rdbmsEntity.entity.getAttribute("qualifiedName") should be ("default." + tableName)

    rdbmsEntity.dependencies.size should be (0)
  }

  test("convert hbase properties to hbase table entity") {
    val cluster = "primary"
    val tableName = "employee"
    val nameSpace = "default"
    val hbaseEntity = external.hbaseTableToEntity(cluster, tableName, nameSpace)

    hbaseEntity.entity.getTypeName should be (external.HBASE_TABLE_STRING)
    hbaseEntity.entity.getAttribute("name") should be (tableName)
    hbaseEntity.entity.getAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE) should be (cluster)
    hbaseEntity.entity.getAttribute("uri") should be (nameSpace + ":" + tableName)

    hbaseEntity.dependencies.size should be (0)
  }

  test("convert s3 path to aws_s3 entities") {
    val pathEntity = external.pathToEntity("s3://testbucket/testpseudodir/testfile")

    pathEntity.entity.getTypeName should be (external.S3_OBJECT_TYPE_STRING)
    pathEntity.entity.getAttribute("name") should be ("testfile")
    pathEntity.entity.getAttribute("qualifiedName") should be (
      "s3://testbucket/testpseudodir/testfile")

    val deps = pathEntity.dependencies
    val dirEntity = deps.find(_.entity.getTypeName == external.S3_PSEUDO_DIR_TYPE_STRING)

    dirEntity.get.entity.getTypeName should be (external.S3_PSEUDO_DIR_TYPE_STRING)
    dirEntity.get.entity.getAttribute("name") should be ("/testpseudodir/")
    dirEntity.get.entity.getAttribute("qualifiedName") should be (
      "s3://testbucket/testpseudodir/")

    pathEntity.entity.getAttribute("pseudoDirectory") should be (
      AtlasUtils.entityToReference(dirEntity.get.entity))

    val bucketEntity = dirEntity.get.dependencies.find { e =>
      e.entity.getTypeName == external.S3_BUCKET_TYPE_STRING
    }

    bucketEntity.get.entity.getTypeName should be (external.S3_BUCKET_TYPE_STRING)
    bucketEntity.get.entity.getAttribute("name") should be ("testbucket")
    bucketEntity.get.entity.getAttribute("qualifiedName") should be (
      "s3://testbucket")

    dirEntity.get.entity.getAttribute("bucket") should be (
      AtlasUtils.entityToReference(bucketEntity.get.entity))

    bucketEntity.get.dependencies.length should be (0)
  }

}

