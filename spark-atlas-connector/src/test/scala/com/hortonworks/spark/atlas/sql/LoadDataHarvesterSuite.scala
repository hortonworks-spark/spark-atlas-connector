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

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.Files
import java.util

import scala.util.Random
import scala.collection.JavaConverters._

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, LoadDataCommand}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import com.hortonworks.spark.atlas.types.external

class LoadDataHarvesterSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _
  private val sourceTblName = "source_" + Random.nextInt(100000)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string)")
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

  test("LOAD DATA [LOCAL] INPATH path source") {
    val file = Files.createTempFile("input", ".txt").toFile
    val out = new PrintWriter(new FileOutputStream(file))
    out.write("a\nb\nc\nd\n")
    out.close()

    val qe = sparkSession.sql(s"LOAD DATA LOCAL INPATH '${file.getAbsolutePath}' " +
      s"OVERWRITE INTO  TABLE $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)
    val node = qe.sparkPlan.collect { case p: LeafExecNode => p }
    assert(node.size == 1)
    val execNode = node.head.asInstanceOf[ExecutedCommandExec]

    val entities = CommandsHarvester.LoadDataHarvester.harvest(
      execNode.cmd.asInstanceOf[LoadDataCommand], qd)
    val pEntity = entities.head

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasEntity]]
    inputs.size() should be (1)

    val inputPath = inputs.asScala.head
    inputPath.getTypeName should be (external.FS_PATH_TYPE_STRING)
    inputPath.getAttribute("name") should be (file.getAbsolutePath.toLowerCase)

    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasEntity]]
    outputs.size() should be (1)
    val outputTable = outputs.asScala.head
    outputTable.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
    outputTable.getAttribute("name") should be (sourceTblName)
    outputTable.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
      s"default.$sourceTblName@primary")
  }
}
