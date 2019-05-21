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

package com.hortonworks.spark.atlas.sql.testhelper

import com.hortonworks.spark.atlas.{AtlasClientConf, SACAtlasReferenceable}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

abstract class BaseHarvesterSuite
  extends FunSuite
  with Matchers
  with ProcessEntityValidator
  with TableEntityValidator {

  protected def getSparkSession: SparkSession

  protected def getDbName: String

  protected def expectSparkTableModels: Boolean

  protected def initializeTestEnvironment(): Unit = {}

  protected def cleanupTestEnvironment(): Unit = {}

  private val atlasClientConf: AtlasClientConf = new AtlasClientConf()
  protected val _clusterName: String = atlasClientConf.get(AtlasClientConf.CLUSTER_NAME)

  protected lazy val _spark: SparkSession = getSparkSession
  protected lazy val _dbName: String = getDbName
  protected lazy val _useSparkTable: Boolean = expectSparkTableModels

  protected def prepareDatabase(): Unit = {
    _spark.sql(s"DROP DATABASE IF EXISTS ${_dbName} Cascade")
    _spark.sql(s"CREATE DATABASE ${_dbName}")
    _spark.sql(s"USE ${_dbName}")
  }

  protected def cleanupDatabase(): Unit = {
    _spark.sql(s"DROP DATABASE IF EXISTS ${_dbName} Cascade")
  }

  protected def assertTable(ref: SACAtlasReferenceable, tableName: String): Unit = {
    assertTable(ref, _dbName, tableName, _clusterName, _useSparkTable)
  }

  protected def assertTableWithNamePrefix(
                                           ref: SACAtlasReferenceable,
                                           tblNamePrefix: String): Unit = {
    assertTableWithNamePrefix(ref, _dbName, tblNamePrefix, _clusterName, _useSparkTable)
  }
}
