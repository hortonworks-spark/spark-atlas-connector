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
import org.apache.atlas.AtlasClient
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExecutedCommandExec}
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.types.external
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.sql.testhelper.ProcessEntityValidator

class CreateViewHarvesterSuite
  extends FunSuite
  with Matchers
  with WithHiveSupport
  with ProcessEntityValidator {
  private val sourceTblName = "source_" + Random.nextInt(100000)
  private val destinationViewName = "destination_" + Random.nextInt(100000)
  private val destinationViewName2 = "destination_" + Random.nextInt(100000)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('lucy'), ('tom')")
  }

  test("CREATE VIEW FROM TABLE") {
    val qe = sparkSession.sql(s"CREATE VIEW $destinationViewName " +
      s"AS SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      val inputEntity = inputs.head.entity
      inputEntity.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      inputEntity.getAttribute("name") should be (sourceTblName)
      inputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should be (
        s"default.$sourceTblName@primary")
    }, outputs => {
      outputs.size should be (1)
      val outputEntity = outputs.head.entity
      outputEntity.getAttribute("name") should be (destinationViewName)
      outputEntity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString should
        endWith (s"default.$destinationViewName")
    })
  }

  test("CREATE VIEW without source") {
    val qe = sparkSession.sql(s"CREATE VIEW $destinationViewName2 " +
      s"AS SELECT 1").queryExecution
    val qd = QueryDetail(qe, 0L, 0L)

    assert(qe.sparkPlan.isInstanceOf[ExecutedCommandExec])
    val node = qe.sparkPlan.asInstanceOf[ExecutedCommandExec]
    assert(node.cmd.isInstanceOf[CreateViewCommand])
    val cmd = node.cmd.asInstanceOf[CreateViewCommand]

    val entities = CommandsHarvester.CreateViewHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (0)
    }, outputs => {
      outputs.size should be (1)
      val outputEntity = outputs.head.entity
      AtlasEntityReadHelper.getQualifiedName(outputEntity) should endWith (
        s"default.$destinationViewName2")
      outputEntity.getAttribute("name") should be (destinationViewName2)
    })
  }
}
