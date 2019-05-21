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

import java.io.{FileOutputStream, PrintWriter}
import java.nio.file.Files

import scala.util.Random
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, LoadDataCommand}
import com.hortonworks.spark.atlas.types.external
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.sql.testhelper.BaseHarvesterSuite
import org.apache.spark.sql.SparkSession

abstract class BaseLoadDataHarvesterSuite
  extends BaseHarvesterSuite {

  protected val sourceTblName = "source_" + Random.nextInt(100000)

  protected override def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceTblName (name string)")
  }

  override protected def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("LOAD DATA [LOCAL] INPATH path source") {
    val file = Files.createTempFile("input", ".txt").toFile
    val out = new PrintWriter(new FileOutputStream(file))
    out.write("a\nb\nc\nd\n")
    out.close()

    val qe = _spark.sql(s"LOAD DATA LOCAL INPATH '${file.getAbsolutePath}' " +
      s"OVERWRITE INTO  TABLE $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L)
    val node = qe.sparkPlan.collect { case p: LeafExecNode => p }
    assert(node.size == 1)
    val execNode = node.head.asInstanceOf[ExecutedCommandExec]

    val entities = CommandsHarvester.LoadDataHarvester.harvest(
      execNode.cmd.asInstanceOf[LoadDataCommand], qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      val inputEntity = inputs.head.asInstanceOf[SACAtlasEntityWithDependencies].entity
      inputEntity.getTypeName should be (external.FS_PATH_TYPE_STRING)
      inputEntity.getAttribute("name") should be (file.getAbsolutePath.toLowerCase)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, _dbName, sourceTblName, _clusterName, _useSparkTable)
    })
  }
}

class LoadDataHarvesterSuite
  extends BaseLoadDataHarvesterSuite
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

class LoadDataHarvesterWithRemoteHMSSuite
  extends BaseLoadDataHarvesterSuite
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
