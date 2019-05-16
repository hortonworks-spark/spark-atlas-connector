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

import com.hortonworks.spark.atlas.sql.testhelper.{BaseHarvesterSuite, FsEntityValidator, ProcessEntityValidator, TableEntityValidator}

import scala.util.Random
import org.apache.atlas.AtlasClient
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.{WithHiveSupport, WithRemoteHiveMetastoreServiceSupport}
import org.apache.spark.sql.SparkSession

abstract class BaseCreateHiveTableAsSelectHarvesterSuite
  extends BaseHarvesterSuite
  with FsEntityValidator {

  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val sourceTbl1Name = "source1_" + Random.nextInt(100000)

  override protected def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceTblName (name string, age int)")
    _spark.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('jerry', 20), ('tom', 15)")
    _spark.sql(s"CREATE TABLE $sourceTbl1Name (name string, salary int)")
    _spark.sql(s"INSERT INTO TABLE $sourceTbl1Name VALUES ('jerry', 10), ('tom', 20)")
  }

  override protected def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("CREATE TABLE dest AS SELECT [] FROM source") {
    val destTblName = "dest1_" + Random.nextInt(100000)
    val qe = _spark.sql(s"CREATE TABLE $destTblName AS SELECT name FROM $sourceTblName")
      .queryExecution
    val qd = QueryDetail(qe, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        _spark.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, sourceTblName)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destTblName)
    })
  }

  test(s"CREATE TABLE dest AS SELECT [] FROM source, source1 WHERE...") {
    val destTblName = "dest2_" + Random.nextInt(100000)
    val qe = _spark.sql(s"CREATE TABLE $destTblName AS " +
      s"SELECT a.age FROM $sourceTblName a, $sourceTbl1Name b WHERE a.name = b.name")
      .queryExecution
    val qd = QueryDetail(qe, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 3)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        _spark.sparkContext.applicationId )
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (2)

      val mayBeSourceTbl = inputs.find(_.qualifiedName.contains(sourceTblName))
      val mayBeSourceTbl1 = inputs.find(_.qualifiedName.contains(sourceTbl1Name))
      assert(mayBeSourceTbl.isDefined)
      assert(mayBeSourceTbl1.isDefined)

      assertTable(mayBeSourceTbl.get, sourceTblName)
      assertTable(mayBeSourceTbl1.get, sourceTbl1Name)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destTblName)
    })
  }

  test("CREATE TABLE dest AS SELECT [] FROM view") {
    val viewName = "view3_" + Random.nextInt(100000)
    _spark.sql(s"CREATE VIEW $viewName AS SELECT name FROM $sourceTblName")

    val destTblName = "dest3_" + Random.nextInt(100000)
    val qe = _spark.sql(s"CREATE TABLE $destTblName AS SELECT * FROM $viewName")
      .queryExecution
    val qd = QueryDetail(qe, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)
    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        _spark.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, sourceTblName)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destTblName)
    })
  }

  test("CREATE TABLE dest AS SELECT [] FROM directory") {
    val destTblName = "dest4_" + Random.nextInt(100000)
    val path =
      new File(this.getClass.getClassLoader.getResource("users.parquet").toURI).getAbsolutePath

    val qe = _spark.sql(s"CREATE TABLE $destTblName AS SELECT * " +
      s"FROM parquet.`$path`").queryExecution
    val qd = QueryDetail(qe, 0L)
    val ctasNode = qe.sparkPlan.collect {
      case p: DataWritingCommandExec => p
      case p: LeafExecNode => p
    }
    assert(ctasNode.size === 2)
    val execNode = ctasNode.head.asInstanceOf[DataWritingCommandExec]

    val entities = CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(
      execNode.cmd.asInstanceOf[CreateHiveTableAsSelectCommand], qd)

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        _spark.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      assertFsEntity(inputs.head, path)
    }, _ => {})
  }
}

class CreateHiveTableAsSelectHarvesterSuite
  extends BaseCreateHiveTableAsSelectHarvesterSuite
  with WithHiveSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override def getSparkSession: SparkSession = sparkSession

  override def getDbName: String = "sac"

  override def expectSparkTableModels: Boolean = true
}

class CreateHiveTableAsSelectHarvesterWithRemoteHMSSuite
  extends BaseCreateHiveTableAsSelectHarvesterSuite
  with WithRemoteHiveMetastoreServiceSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override def getSparkSession: SparkSession = sparkSession

  override def expectSparkTableModels: Boolean = false

  override def getDbName: String = dbName
}
