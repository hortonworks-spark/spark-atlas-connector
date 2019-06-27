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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait WithHDFSSupport extends BeforeAndAfterAll { self: Suite =>

  protected var sparkSession: SparkSession = _

  private var hdfsCluster: MiniDFSCluster = _
  protected var hdfsURI: String = _

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

    val baseDir = new File("./target/hdfs/").getAbsoluteFile()
    FileUtil.fullyDelete(baseDir)

    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    val builder = new MiniDFSCluster.Builder(conf)

    hdfsCluster = builder.build()
    hdfsURI = s"hdfs://localhost:${hdfsCluster.getNameNodePort()}/"

    sparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getCanonicalName)
      .enableHiveSupport()
      .config("spark.hadoop.fs.defaultFS", hdfsURI)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.sessionState.catalog.reset()
      sparkSession.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      sparkSession = null
    }
    System.clearProperty("spark.driver.port")

    hdfsCluster.shutdown(true)

    super.afterAll()
  }
}
