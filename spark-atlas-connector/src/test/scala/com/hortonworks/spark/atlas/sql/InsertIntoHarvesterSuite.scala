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
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}

import com.hortonworks.spark.atlas.types.external

class InsertIntoHarvesterSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _
  private val sourceHiveTblName = "source_h_" + Random.nextInt(100000)
  private val sourceSparkTblName = "source_s_" + Random.nextInt(100000)
  private val destinationHiveTblName = "destination_h_" + Random.nextInt(100000)
  private val destinationSparkTblName = "destination_s_" + Random.nextInt(100000)

  private val inputTable1 = "input1_" + Random.nextInt(100000)
  private val inputTable2 = "input2_" + Random.nextInt(100000)
  private val inputTable3 = "input3_" + Random.nextInt(100000)
  private val inputTable4 = "input4_" + Random.nextInt(100000)
  private val outputTable1 = "output1_" + Random.nextInt(100000)
  private val outputTable2 = "output2_" + Random.nextInt(100000)
  private val outputTable3 = "output3_" + Random.nextInt(100000)
  private val outputTable4 = "output4_" + Random.nextInt(100000)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql(s"CREATE TABLE $sourceHiveTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceHiveTblName VALUES ('a'), ('b'), ('c')")

    sparkSession.sql(s"CREATE TABLE $sourceSparkTblName (name string) USING ORC")
    sparkSession.sql(s"INSERT INTO TABLE $sourceSparkTblName VALUES ('d'), ('e'), ('f')")

    sparkSession.sql(s"CREATE TABLE $destinationHiveTblName (name string)")
    sparkSession.sql(s"CREATE TABLE $destinationSparkTblName (name string) USING ORC")

    // multiple source tables
    sparkSession.sql(s"CREATE TABLE $inputTable1 (a int, b string)")
    sparkSession.sql(s"INSERT INTO $inputTable1 VALUES(1, 'str1')")
    sparkSession.sql(s"CREATE TABLE $inputTable2 (c int, d string)")
    sparkSession.sql(s"INSERT INTO $inputTable2 VALUES(1, 'str2')")
    sparkSession.sql(s"CREATE TABLE $inputTable3 (e int, f string)")
    sparkSession.sql(s"INSERT INTO $inputTable3 VALUES(1, 'str3')")
    sparkSession.sql(s"CREATE TABLE $outputTable1 (a int, b string, c int, d string, e int, f string)")

    // multiple destination tables
    sparkSession.sql(s"create table $inputTable4 (id int, code string, salary int)")
    sparkSession.sql(s"INSERT INTO $inputTable4 VALUES(1, 'hihi', 100)")
    sparkSession.sql(s"create table $outputTable2 (id int)")
    sparkSession.sql(s"create table $outputTable3 (code string)")
    sparkSession.sql(s"create table $outputTable4 (salary int)")
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

  test("INSERT INTO HIVE TABLE FROM HIVE TABLE") {
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationHiveTblName " +
      s"SELECT * FROM $sourceHiveTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTbl.getAttribute("name") should be (sourceHiveTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$sourceHiveTblName@primary")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTbl = outputs.asScala.head
    outputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTbl.getAttribute("name") should be (destinationHiveTblName)
    outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$destinationHiveTblName@primary")
  }

  test("INSERT INTO HIVE TABLE FROM SPARK TABLE") {
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationHiveTblName " +
      s"SELECT * FROM $sourceSparkTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should not be external.HIVE_TABLE_TYPE_STRING
    inputTbl.getAttribute("name") should be (sourceSparkTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$sourceSparkTblName")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTbl = outputs.asScala.head
    outputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTbl.getAttribute("name") should be (destinationHiveTblName)
    outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$destinationHiveTblName@primary")
  }

  test("INSERT INTO SPARK TABLE FROM HIVE TABLE") {
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationSparkTblName " +
      s"SELECT * FROM $sourceHiveTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

    val entities = CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTbl.getAttribute("name") should be (sourceHiveTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$sourceHiveTblName@primary")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTbl = outputs.asScala.head
    outputTbl.getTypeName should not be external.HIVE_TABLE_TYPE_STRING
    outputTbl.getAttribute("name") should be (destinationSparkTblName)
    outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$destinationSparkTblName")
  }

  test("INSERT INTO SPARK TABLE FROM SPARK TABLE") {
    val qe = sparkSession.sql(s"INSERT INTO TABLE $destinationSparkTblName " +
      s"SELECT * FROM $sourceSparkTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]

    val entities = CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should not be external.HIVE_TABLE_TYPE_STRING
    inputTbl.getAttribute("name") should be (sourceSparkTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$sourceSparkTblName")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTbl = outputs.asScala.head
    outputTbl.getTypeName should not be external.HIVE_TABLE_TYPE_STRING
    outputTbl.getAttribute("name") should be (destinationSparkTblName)
    outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should endWith (
      s"default.$destinationSparkTblName")
  }

  test("INSERT INTO TABLE FROM MULTIPLE TABLES") {
    val qe = sparkSession.sql(s"INSERT INTO $outputTable1 " +
      s"SELECT * FROM $inputTable1, $inputTable2, $inputTable3 " +
      s"where $inputTable1.a = $inputTable2.c AND $inputTable1.a = $inputTable3.e").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (3)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTbl.getAttribute("name").toString should startWith ("input")
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should startWith (
      "default.input")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTbl = outputs.asScala.head
    outputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTbl.getAttribute("name") should be (outputTable1)
    outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$outputTable1@primary")
  }

  test("INSERT INTO MULTIPLE TABLES FROM TABLE") {
    val qe = sparkSession.sql(s"FROM $inputTable4 " +
      s"INSERT INTO TABLE $outputTable2 SELECT $inputTable4.id " +
      s"INSERT INTO TABLE $outputTable3 SELECT $inputTable4.code " +
      s"INSERT into TABLE $outputTable4 SELECT $inputTable4.salary").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[UnionExec])
    qe.sparkPlan.asInstanceOf[UnionExec]
    qe.sparkPlan.children.foreach(child => {
      assert(child.isInstanceOf[DataWritingCommandExec])
      val node = child.asInstanceOf[DataWritingCommandExec]
      assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
      val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

      val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
      val pEntity = entities.head

      assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
      val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
      inputs.size() should be (1)

      val inputTbl = inputs.asScala.head
      inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      inputTbl.getAttribute("name") should be (inputTable4)
      inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$inputTable4@primary")

      assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
      val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
      outputs.size() should be (1)
      val outputTbl = outputs.asScala.head
      outputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      outputTbl.getAttribute("name").toString should startWith ("output")
      outputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should startWith (
        "default.output")
    })
  }
}
