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

package com.hortonworks.spark.atlas

import java.io.File
import java.nio.file.Files

import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hotels.beeju.ThriftHiveMetaStoreTestUtil
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithRemoteHiveMetastoreServiceSupport extends BeforeAndAfterAll { self: Suite =>
  protected val dbName = "sac_hive_metastore"

  protected var sparkSession: SparkSession = _

  private var warehouseDir: String = _

  private val hive = new ThriftHiveMetaStoreTestUtil(dbName)

  private def cleanupAnyExistingSession(): Unit = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (session.isDefined) {
      session.get.sessionState.catalog.reset()
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    cleanupAnyExistingSession()

    hive.before()

    warehouseDir = Files.createTempDirectory("sac-warehouse-").toString
    sparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getCanonicalName)
      .enableHiveSupport()
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.hadoop.hive.metastore.uris", hive.getThriftConnectionUri)
      .getOrCreate()

    // reset hiveConf to make sure the configuration change takes effect
    SparkUtils.resetHiveConf
  }

  override protected def afterAll(): Unit = {
    try {
      hive.after()
      sparkSession.sessionState.catalog.reset()
      sparkSession.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      // reset hiveConf again to prevent affecting other tests
      SparkUtils.resetHiveConf

      sparkSession = null
      FileUtils.deleteDirectory(new File(warehouseDir))
    }
    System.clearProperty("spark.driver.port")

    super.afterAll()
  }
}
