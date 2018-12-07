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

import com.hortonworks.spark.atlas.WithHiveSupport
import com.hortonworks.spark.atlas.utils.SparkUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType
import org.scalatest.{FunSuite, Matchers}


class CreateDataSourceTableAsSelectHarvesterSuite
    extends FunSuite with Matchers with WithHiveSupport {

  private val sourceTblName = "source_" + Random.nextInt(100000)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession.sql(s"CREATE TABLE $sourceTblName (name string, age int)")
  }

  test("saveAsTable should have output entity having table details") {
    val destTblName = "dest1_" + Random.nextInt(100000)
    val df = sparkSession.sql(s"SELECT * FROM $sourceTblName")

    // The codes below look after DataFrameWriter.saveAsTable codes as of Spark 2.4.
    // It uses internal APIs for this test. If the compatibility is broken, we should better
    // just remove this test.
    val tableIdent = df.sparkSession.sessionState.sqlParser.parseTableIdentifier(destTblName)
    val storage = DataSource.buildStorageFormatFromOptions(Map("path" -> "/tmp/foo"))
    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = new StructType,
      provider = Some("parquet"),
      partitionColumnNames = Nil,
      bucketSpec = None)
    val cmd = CreateDataSourceTableAsSelectCommand(
      tableDesc,
      SaveMode.ErrorIfExists,
      df.queryExecution.logical,
      Seq("name", "age"))
    val newTable = tableDesc.copy(
      storage = tableDesc.storage.copy(),
      schema = df.schema)
    sparkSession.sessionState.catalog.createTable(
      newTable, ignoreIfExists = false)

    val qd = QueryDetail(df.queryExecution, 0L, 0L)
    val entities = CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(cmd, qd)
    val maybeEntity = entities.find(e => e.getTypeName == "spark_table")

    assert(maybeEntity.isDefined, s"Output entity for table [$destTblName] was not found.")
    assert(maybeEntity.get.getAttribute("name") == destTblName)
    assert(maybeEntity.get.getAttribute("owner") == SparkUtils.currUser())
  }
}
