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
import java.util

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.types.{external, metadata}

class CreateHiveTableAsSelectHarvesterSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _
  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val sourceTbl1Name = "source1_" + Random.nextInt(100000)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string, age int)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('jerry', 20), ('tom', 15)")
    sparkSession.sql(s"CREATE TABLE $sourceTbl1Name (name string, salary int)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTbl1Name VALUES ('jerry', 10), ('tom', 20)")
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null

    FileUtils.deleteDirectory(new File("metastore_db"))
    FileUtils.deleteDirectory(new File("spark-warehouse"))

    super.afterAll()
  }

  test("CREATE TABLE dest AS SELECT [] FROM source") {
    val destTblName = "dest1_" + Random.nextInt(100000)
    val qe = sparkSession.sql(s"CREATE TABLE $destTblName AS SELECT name FROM $sourceTblName")
      .queryExecution
    val qd = QueryDetail(qe, 0L, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)
    val pEntity = entities.head

    pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId)
    pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)
    val inputTable = inputs.asScala.head
    inputTable.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTable.getAttribute("name") should be (sourceTblName)
    inputTable.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$sourceTblName@primary")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTable = outputs.asScala.head
    outputTable.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTable.getAttribute("name") should be (destTblName)
    outputTable.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$destTblName@primary")
  }

  test(s"CREATE TABLE dest AS SELECT [] FROM source, source1 WHERE...") {
    val destTblName = "dest2_" + Random.nextInt(100000)
    val qe = sparkSession.sql(s"CREATE TABLE $destTblName AS " +
      s"SELECT a.age FROM $sourceTblName a, $sourceTbl1Name b WHERE a.name = b.name")
      .queryExecution
    val qd = QueryDetail(qe, 0L, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 3)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)
    val pEntity = entities.head

    pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId )
    pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (2)
    inputs.asScala.map(_.getAttribute("name")).toSet should be (Set(sourceTblName, sourceTbl1Name))

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTable = outputs.asScala.head
    outputTable.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTable.getAttribute("name") should be (destTblName)
    outputTable.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$destTblName@primary")
  }

  test("CREATE TABLE dest AS SELECT [] FROM view") {
    val viewName = "view3_" + Random.nextInt(100000)
    sparkSession.sql(s"CREATE VIEW $viewName AS SELECT name FROM $sourceTblName")

    val destTblName = "dest3_" + Random.nextInt(100000)
    val qe = sparkSession.sql(s"CREATE TABLE $destTblName AS SELECT * FROM $viewName")
      .queryExecution
    val qd = QueryDetail(qe, 0L, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)
    val pEntity = entities.head

    pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      sparkSession.sparkContext.applicationId)
    pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)
    inputs.asScala.map(_.getAttribute("name")).toSet should be (Set(sourceTblName))

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTable = outputs.asScala.head
    outputTable.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTable.getAttribute("name") should be (destTblName)
    outputTable.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$destTblName@primary")
  }

  test("CREATE TABLE dest AS SELECT [] FROM directory") {
    val destTblName = "dest4_" + Random.nextInt(100000)
    val path =
      new File(this.getClass.getClassLoader.getResource("users.parquet").toURI).getAbsolutePath

    val qe = sparkSession.sql(s"CREATE TABLE $destTblName AS SELECT * " +
      s"FROM parquet.`$path`").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)
    val pEntity = entities.head

    pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
     sparkSession.sparkContext.applicationId)
    pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)
    val inputPath = inputs.asScala.head
    inputPath.getTypeName should be (external.FS_PATH_TYPE_STRING)
    inputPath.getAttribute("name") should be (path.toLowerCase)
  }
}

