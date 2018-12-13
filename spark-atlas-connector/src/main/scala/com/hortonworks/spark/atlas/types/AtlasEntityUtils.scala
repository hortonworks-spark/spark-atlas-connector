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
import org.apache.spark.sql.types.StructType
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.utils.SparkUtils
import org.apache.spark.ml.Pipeline

trait AtlasEntityUtils {

  def conf: AtlasClientConf

  def clusterName: String = conf.get(AtlasClientConf.CLUSTER_NAME)

  def dbType: String = {
    if (SparkUtils.isHiveEnabled()) {
      external.HIVE_DB_TYPE_STRING
    } else {
      metadata.DB_TYPE_STRING
    }
  }

  def dbToEntities(dbDefinition: CatalogDatabase): Seq[AtlasEntity] = {
    if (SparkUtils.isHiveEnabled()) {
      external.hiveDbToEntities(dbDefinition, clusterName, SparkUtils.currUser())
    } else {
      internal.sparkDbToEntities(dbDefinition, clusterName, SparkUtils.currUser())
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

  def storageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String,
      isHiveTable: Boolean): Seq[AtlasEntity] = {
    if (isHiveTable) {
      external.hiveStorageDescToEntities(storageFormat, clusterName, db, table)
    } else {
      internal.sparkStorageFormatToEntities(storageFormat, db, table)
    }
  }

  def storageFormatUniqueAttribute(db: String, table: String, isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.hiveStorageDescUniqueAttribute(clusterName, db, table)
    } else {
      internal.sparkStorageFormatUniqueAttribute(db, table)
    }
  }

  def columnType(isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.HIVE_COLUMN_TYPE_STRING
    } else {
      metadata.COLUMN_TYPE_STRING
    }
  }

  def schemaToEntities(
      schema: StructType,
      db: String,
      table: String,
      isHiveTable: Boolean): List[AtlasEntity] = {
    if (isHiveTable) {
      external.hiveSchemaToEntities(schema, clusterName, db, table)
    } else {
      internal.sparkSchemaToEntities(schema, db, table)
    }
  }

  def columnUniqueAttribute(
      db: String,
      table: String,
      col: String,
      isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.hiveColumnUniqueAttribute(clusterName, db, table, col)
    } else {
      internal.sparkColumnUniqueAttribute(db, table, col)
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

  def tableToEntities(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    if (isHiveTable(tableDefinition)) {
      external.hiveTableToEntities(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntities(tableDefinition, clusterName, mockDbDefinition)
    }
  }

  def tableToEntitiesForAlterTable(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    if (isHiveTable(tableDefinition)) {
      external.hiveTableToEntitiesForAlterTable(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntitiesForAlterTable(tableDefinition, clusterName, mockDbDefinition)
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
      query: Option[String] = None): AtlasEntity =
    internal.sparkProcessToEntity(qe, executionId, executionTime, inputs, outputs, query)

  def processUniqueAttribute(executionId: Long): String =
    internal.sparkProcessUniqueAttribute(executionId)

  // If there is cycle, return empty output entity list
  def cleanOutput(inputs: Seq[AtlasEntity], outputs: Seq[AtlasEntity]): List[AtlasEntity] = {
    val qualifiedNames = inputs.map(e => e.getAttribute("qualifiedName"))
    val isCycle = outputs.exists(x => qualifiedNames.contains(x.getAttribute("qualifiedName")))
    if (isCycle) {
      List.empty
    } else {
      outputs.toList
    }
  }
}
