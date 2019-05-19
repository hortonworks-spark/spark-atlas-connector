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
import org.apache.atlas.AtlasClient
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand
import org.scalatest.{FunSuite, Matchers}
import com.hortonworks.spark.atlas.types.external
import com.hortonworks.spark.atlas.WithHiveSupport
import com.hortonworks.spark.atlas.sql.testhelper.ProcessEntityValidator


class InsertIntoHiveDirHarvesterSuite
  extends FunSuite
  with Matchers
  with WithHiveSupport
  with ProcessEntityValidator {

  private val sourceTblName = "source_" + Random.nextInt(100000)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string)")
    sparkSession.sql(s"INSERT INTO TABLE $sourceTblName VALUES ('a'), ('b'), ('c')")
  }

  test("INSERT OVERWRITE DIRECTORY path...") {
    val qe = sparkSession.sql(s"INSERT OVERWRITE DIRECTORY 'target/dir1' " +
      s"SELECT * FROM $sourceTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveDirCommand])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveDirCommand]

    val entities = CommandsHarvester.InsertIntoHiveDirHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      val inputTbl = inputs.head.entity
      inputTbl.getTypeName should be (external.HIVE_TABLE_TYPE_STRING)
      inputTbl.getAttribute("name") should be (sourceTblName)
      inputTbl.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME) should be (
        s"default.$sourceTblName@primary")
    }, outputs => {
      outputs.size should be (1)
      val outputPath = outputs.head.entity
      outputPath.getTypeName should be (external.FS_PATH_TYPE_STRING)
      val dir = new File("target/dir1").getAbsolutePath
      outputPath.getAttribute("name") should be (dir.toLowerCase)
    })
  }
}
