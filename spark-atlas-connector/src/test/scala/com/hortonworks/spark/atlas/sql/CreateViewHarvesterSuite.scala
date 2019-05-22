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

import scala.util.Random
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExecutedCommandExec}
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.sql.testhelper.BaseHarvesterSuite
import org.apache.spark.sql.SparkSession

abstract class BaseCreateViewHarvesterSuite
  extends BaseHarvesterSuite {

  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val destinationViewName = "destination_" + Random.nextInt(100000)
  private val destinationViewName2 = "destination_" + Random.nextInt(100000)

  protected override def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceTblName (name string)")
    _spark.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('lucy'), ('tom')")
  }

  protected override def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("CREATE VIEW FROM TABLE") {
    val qe = _spark.sql(s"CREATE VIEW $destinationViewName " +
      s"AS SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, sourceTblName)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destinationViewName)
    })
  }

  test("CREATE VIEW without source") {
    val qe = _spark.sql(s"CREATE VIEW $destinationViewName2 " +
      s"AS SELECT 1").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (0)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destinationViewName2)
    })
  }
}

class CreateViewHarvesterSuite
  extends BaseCreateViewHarvesterSuite
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

class CreateViewHarvesterWithRemoteHMSSuite
  extends BaseCreateViewHarvesterSuite
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
