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

import com.hortonworks.spark.atlas.sql.testhelper.ProcessEntityValidator

import scala.util.Random
import org.apache.atlas.AtlasClient
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.WithHiveSupport
import com.hortonworks.spark.atlas.utils.SparkUtils


class CreateHiveTableAsSelectHarvesterSuite
  extends FunSuite
  with Matchers
  with WithHiveSupport
  with ProcessEntityValidator {

  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val sourceTbl1Name = "source1_" + Random.nextInt(100000)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string, age int)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('jerry', 20), ('tom', 15)")
    sparkSession.sql(s"CREATE TABLE $sourceTbl1Name (name string, salary int)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTbl1Name VALUES ('jerry', 10), ('tom', 20)")
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

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        sparkSession.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      val inputEntity = inputs.head.entity
      inputEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      inputEntity.getAttribute("name") should be (sourceTblName)
      inputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$sourceTblName@primary")
    }, outputs => {
      outputs.size should be (1)
      val outputEntity = outputs.head.entity
      outputEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      outputEntity.getAttribute("name") should be (destTblName)
      outputEntity.getAttribute("owner") should be (SparkUtils.currUser())
      outputEntity.getAttribute("ownerType") should be ("USER")
      outputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$destTblName@primary")
    })
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

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        sparkSession.sparkContext.applicationId )
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (2)
      inputs.map(_.entity.getAttribute("name")).toSet should be (
        Set(sourceTblName, sourceTbl1Name))
    }, outputs => {
      outputs.size should be (1)
      val outputEntity = outputs.head.entity
      outputEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      outputEntity.getAttribute("name") should be (destTblName)
      outputEntity.getAttribute("owner") should be (SparkUtils.currUser())
      outputEntity.getAttribute("ownerType") should be ("USER")
      outputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$destTblName@primary")
    })
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
    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        sparkSession.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      inputs.map(_.entity.getAttribute("name")).toSet should be (Set(sourceTblName))
    }, outputs => {
      outputs.size should be (1)
      val outputEntity = outputs.head.entity
      outputEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      outputEntity.getAttribute("name") should be (destTblName)
      outputEntity.getAttribute("owner") should be (SparkUtils.currUser())
      outputEntity.getAttribute("ownerType") should be ("USER")
      outputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$destTblName@primary")
    })
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

    validateProcessEntity(entities.head, pEntity => {
      pEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        sparkSession.sparkContext.applicationId)
      pEntity.getTypeName should be (metadata.PROCESS_TYPE_STRING)
    }, inputs => {
      inputs.size should be (1)
      val inputEntity = inputs.head.entity
      inputEntity.getTypeName should be (external.FS_PATH_TYPE_STRING)
      inputEntity.getAttribute("name") should be (path.toLowerCase)
    }, _ => {})
  }
}

