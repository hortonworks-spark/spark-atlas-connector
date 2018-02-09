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
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.types.external

class InsertIntoHiveDirHarvesterSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _
  private val sourceTblName = "source_" + Random.nextInt(100000)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('a'), ('b'), ('c')")
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

  test("INSERT OVERWRITE DIRECTORY path...") {
    val qe = sparkSession.sql(s"INSERT OVERWRITE DIRECTORY 'target/dir1' " +
      s"SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveDirCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveDirCommand]

    val entities = CommandsHarvester.InsertIntoHiveDirHarvester.harvest(
      cmd, qd)

    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputTbl = inputs.asScala.head
    inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    inputTbl.getAttribute("name") should be (sourceTblName)
    inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$sourceTblName@primary")

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputPath = outputs.asScala.head
    outputPath.getTypeName should be (external.FS_PATH_TYPE_STRING)
    val dir = new File("target/dir1").getAbsolutePath
    outputPath.getAttribute("name") should be (dir.toLowerCase)
  }
}
