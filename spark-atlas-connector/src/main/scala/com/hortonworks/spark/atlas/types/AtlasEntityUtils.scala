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

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasEntityWithDependencies}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.ml.Pipeline

trait AtlasEntityUtils extends Logging {

  def conf: AtlasClientConf

  def clusterName: String = conf.get(AtlasClientConf.CLUSTER_NAME)

  def dbType: String = {
    if (SparkUtils.isHiveEnabled()) {
      external.HIVE_DB_TYPE_STRING
    } else {
      metadata.DB_TYPE_STRING
    }
  }

  def dbToEntity(dbDefinition: CatalogDatabase): AtlasEntityWithDependencies = {
    if (SparkUtils.isHiveEnabled()) {
      external.hiveDbToEntity(dbDefinition, clusterName, SparkUtils.currUser())
    } else {
      internal.sparkDbToEntity(dbDefinition, clusterName, SparkUtils.currUser())
    }
  }

  def dbUniqueAttribute(db: String): String = {
    if (SparkUtils.isHiveEnabled()) {
      external.hiveDbUniqueAttribute(clusterName, db)
    } else {
      internal.sparkDbUniqueAttribute(db)
    }
  }

  def storageFormatType(isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.HIVE_STORAGEDESC_TYPE_STRING
    } else {
      metadata.STORAGEDESC_TYPE_STRING
    }
  }

  def storageFormatToEntity(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String,
      isHiveTable: Boolean): AtlasEntityWithDependencies = {
    if (isHiveTable) {
      external.hiveStorageDescToEntity(storageFormat, clusterName, db, table)
    } else {
      internal.sparkStorageFormatToEntity(storageFormat, db, table)
    }
  }

  def storageFormatUniqueAttribute(db: String, table: String, isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.hiveStorageDescUniqueAttribute(clusterName, db, table)
    } else {
      internal.sparkStorageFormatUniqueAttribute(db, table)
    }
  }

  def tableType(isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.HIVE_TABLE_TYPE_STRING
    } else {
      metadata.TABLE_TYPE_STRING
    }
  }

  def isHiveTable(tableDefinition: CatalogTable): Boolean =
    tableDefinition.provider.contains("hive")

  def tableToEntity(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): AtlasEntityWithDependencies = {
    if (isHiveTable(tableDefinition)) {
      external.hiveTableToEntity(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntity(tableDefinition, clusterName, mockDbDefinition)
    }
  }

  def tableToEntityForAlterTable(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): AtlasEntityWithDependencies = {
    if (isHiveTable(tableDefinition)) {
      external.hiveTableToEntitiesForAlterTable(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntityForAlterTable(tableDefinition, clusterName, mockDbDefinition)
    }
  }

  def tableUniqueAttribute(db: String, table: String, isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.hiveTableUniqueAttribute(clusterName, db, table)
    } else {
      internal.sparkTableUniqueAttribute(db, table)
    }
  }

  def pipelineUniqueAttribute(pipeline: Pipeline): String = {
    pipeline.uid
  }

  def processType: String = metadata.PROCESS_TYPE_STRING

  def processToEntity(
      qe: QueryExecution,
      executionId: Long,
      executionTime: Long,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      query: Option[String] = None): AtlasEntityWithDependencies =
    internal.sparkProcessToEntity(qe, executionId, executionTime, inputs, outputs, query)

  def processUniqueAttribute(executionId: Long): String =
    internal.sparkProcessUniqueAttribute(executionId)

  // If there is cycle, return empty output entity list
  def cleanOutput(
      inputs: Seq[AtlasEntityWithDependencies],
      outputs: Seq[AtlasEntityWithDependencies]): List[AtlasEntityWithDependencies] = {
    val qualifiedNames = inputs.map(e => e.entity.getAttribute("qualifiedName"))
    val isCycle = outputs.exists { x =>
      qualifiedNames.contains(x.entity.getAttribute("qualifiedName"))
    }
    if (isCycle) {
      logWarn("Detected cycle - same entity observed to both input and output. " +
        "Discarding output entities as Atlas doesn't support cycle.")
      List.empty
    } else {
      outputs.toList
    }
  }
}
