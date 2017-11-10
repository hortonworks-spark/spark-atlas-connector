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

import org.apache.hadoop.conf.Configuration

import scala.util.control.NonFatal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog

object SparkUtils extends Logging {
  @volatile private var prefix: String = null

  lazy val sparkSession: SparkSession = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (session.isEmpty) {
      throw new IllegalStateException("Cannot find active or default SparkSession in the current " +
        "context")
    }

    session.get
  }

  lazy val hiveConf: Configuration = {
    try {
      new HiveConf(sparkSession.sparkContext.hadoopConfiguration, classOf[HiveConf])
    } catch {
      case NonFatal(e) =>
        logWarn(s"Fail to create Hive Configuration", e)
        sparkSession.sparkContext.hadoopConfiguration
    }
  }

  /**
   * Identify a unique qualified prefix based on how catalog is used. This is to differentiate
   * multiple same-name DBs/tables in Atlas graph store when we have multiple catalogs/metastores.
   * For example if we use in-memory catalog which is application based, so we will potentially have
   * multiple same-name DBs stored in Atlas, a unique prefix "app-id" is used to differentiate
   * these duplications.
   */
  def getUniqueQualifiedPrefix(): String = {
    if (prefix == null) {
      SparkUtils.synchronized {
        if (prefix == null) {
          prefix = if (sparkSession.sparkContext.getConf
            .get("spark.sql.catalogImplementation", "hive") == "in-memory") {
            sparkSession.sparkContext.applicationId + "."
          } else if (hiveConf.getTrimmed("hive.metastore.uris", "").nonEmpty) {
            // If we're using remote Metastore service, then a unique prefix is identified by
            // metastore uris.
            hiveConf.getTrimmed("hive.metastore.uris") + "."
          } else if (hiveConf.get("javax.jdo.option.ConnectionURL", "").nonEmpty) {
            // If we're using local metastore, then a unique prefix is identified by the backend
            // metastore database.
            hiveConf.get("javax.jdo.option.ConnectionURL") + "."
          } else {
            // If nothing is configured, which means we're using embedded derby metastore, which
            // is appliation based, so we should differentiate by app-id.
            sparkSession.sparkContext.applicationId + "."
          }
        }
      }
    }

    prefix
  }

  /**
   * Get the external catalog of current active SparkSession.
   */
  def getExternalCatalog(): ExternalCatalog = {
    val catalog = sparkSession.sharedState.externalCatalog
    require(catalog != null, "catalog is null")
    catalog
  }
}
