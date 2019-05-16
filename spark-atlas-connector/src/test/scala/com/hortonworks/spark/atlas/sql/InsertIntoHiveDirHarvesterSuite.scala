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

import scala.util.Random
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand
import com.hortonworks.spark.atlas.{WithHiveSupport, WithRemoteHiveMetastoreServiceSupport}
import com.hortonworks.spark.atlas.sql.testhelper.{BaseHarvesterSuite, FsEntityValidator}
import org.apache.spark.sql.SparkSession

abstract class BaseInsertIntoHiveDirHarvesterSuite
  extends BaseHarvesterSuite
  with FsEntityValidator {

  private val sourceTblName = "source_" + Random.nextInt(100000)

  protected override def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceTblName (name string)")
    _spark.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('a'), ('b'), ('c')")
  }

  override protected def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("INSERT OVERWRITE DIRECTORY path...") {
    val qe = _spark.sql(s"INSERT OVERWRITE DIRECTORY 'target/dir1' " +
      s"SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveDirCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveDirCommand]

    val entities = CommandsHarvester.InsertIntoHiveDirHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, _dbName, sourceTblName, _clusterName, _useSparkTable)
    }, outputs => {
      outputs.size should be (1)
      val dir = new File("target/dir1").getAbsolutePath
      assertFsEntity(outputs.head, dir)
    })
  }
}

class InsertIntoHiveDirHarvesterSuite
  extends BaseInsertIntoHiveDirHarvesterSuite
  with WithHiveSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override protected def getSparkSession: SparkSession = sparkSession

  override protected def getDbName: String = "sac"

  override protected def expectSparkTableModels: Boolean = true
}

class InsertIntoHiveDirHarvesterWithRemoteHMSSuite
  extends BaseInsertIntoHiveDirHarvesterSuite
  with WithRemoteHiveMetastoreServiceSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override protected def getSparkSession: SparkSession = sparkSession

  override protected def getDbName: String = dbName

  override protected def expectSparkTableModels: Boolean = false
}
