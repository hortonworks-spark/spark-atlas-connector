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

package com.hortonworks.spark.atlas.utils

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkUtilsSuite extends FunSuite with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = _

  after {
    if (sparkSession != null) {
      sparkSession.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      sparkSession = null
    }
  }

  test("get unique prefix when using in-memory catalog") {
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()

    SparkUtils.getUniqueQualifiedPrefix() should be (sparkSession.sparkContext.applicationId + ".")
  }

  // TODO. Should have a better way to figure out unique name
  ignore("get unique prefix when using hive catalog") {
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    val hiveConf = new HiveConf(sparkSession.sparkContext.hadoopConfiguration, classOf[HiveConf])

    // if hive.metastore.uris is set, which means we're using metastore server.
    hiveConf.set("hive.metastore.uris", "thrift://localhost:10000")
    SparkUtils.getUniqueQualifiedPrefix(Some(hiveConf)) should be ("thrift://localhost:10000.")

    // if embedded mode is used
    hiveConf.unset("hive.metastore.uris")
    hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    SparkUtils.getUniqueQualifiedPrefix(Some(hiveConf)) should be (
      sparkSession.sparkContext.applicationId + ".")

    // otherwise if local metastore backend is used
    hiveConf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    hiveConf.set("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost:3030/hive?createDatabaseIfNotExist=true")
    SparkUtils.getUniqueQualifiedPrefix(Some(hiveConf)) should be (
      "jdbc:mysql://localhost:3030/hive?createDatabaseIfNotExist=true.")
  }
}
