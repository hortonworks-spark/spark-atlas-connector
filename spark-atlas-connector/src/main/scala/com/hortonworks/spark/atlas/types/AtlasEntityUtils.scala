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

package com.hortonworks.spark.atlas.types

import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import com.hortonworks.spark.atlas.{AtlasClientConf, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.ml.Pipeline

trait AtlasEntityUtils extends Logging {

  def conf: AtlasClientConf

  def clusterName: String = conf.get(AtlasClientConf.CLUSTER_NAME)

  def sparkDbType: String = metadata.DB_TYPE_STRING

  def sparkDbToEntity(dbDefinition: CatalogDatabase): SACAtlasEntityWithDependencies = {
    internal.sparkDbToEntity(dbDefinition, clusterName, SparkUtils.currUser())
  }

  def sparkDbUniqueAttribute(db: String): String = {
    internal.sparkDbUniqueAttribute(db)
  }

  def sparkStorageFormatType: String = metadata.STORAGEDESC_TYPE_STRING

  def sparkStorageFormatToEntity(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): SACAtlasEntityWithDependencies = {
    internal.sparkStorageFormatToEntity(storageFormat, db, table)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    internal.sparkStorageFormatUniqueAttribute(db, table)
  }

  def sparkTableType: String = metadata.TABLE_TYPE_STRING

  def tableToEntity(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    if (SparkUtils.usingRemoteMetastoreService()) {
      external.hiveTableToReference(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntity(tableDefinition, clusterName, mockDbDefinition)
    }
  }

  def sparkTableToEntity(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    internal.sparkTableToEntity(tableDefinition, clusterName, mockDbDefinition)
  }

  def sparkTableToEntityForAlterTable(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    internal.sparkTableToEntityForAlterTable(tableDefinition, clusterName, mockDbDefinition)
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    internal.sparkTableUniqueAttribute(db, table)
  }

  def pipelineUniqueAttribute(pipeline: Pipeline): String = {
    pipeline.uid
  }

  def processType: String = metadata.PROCESS_TYPE_STRING

  def processUniqueAttribute(executionId: Long): String =
    internal.sparkProcessUniqueAttribute(executionId)

  // If there is cycle, return empty output entity list
  def cleanOutput(
                   inputs: Seq[SACAtlasReferenceable],
                   outputs: Seq[SACAtlasReferenceable]): List[SACAtlasReferenceable] = {
    val qualifiedNames = inputs.map(_.qualifiedName)
    val isCycle = outputs.exists(x => qualifiedNames.contains(x.qualifiedName))
    if (isCycle) {
      logWarn("Detected cycle - same entity observed to both input and output. " +
        "Discarding output entities as Atlas doesn't support cycle.")
      List.empty
    } else {
      outputs.toList
    }
  }
}
