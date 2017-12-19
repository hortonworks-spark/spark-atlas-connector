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

<<<<<<< HEAD

import java.io.File
import java.net.{URI, URISyntaxException}

import scala.collection.JavaConverters._

import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.execution.QueryExecution
=======
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.utils.SparkUtils
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
>>>>>>> Add Hive support and refactor the code
import org.apache.spark.sql.types.StructType

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
      external.hiveDbToEntities(dbDefinition, clusterName)
    } else {
      internal.sparkDbToEntities(dbDefinition)
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
      metadata.DB_TYPE_STRING
    }
  }

  def storageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
<<<<<<< HEAD
      table: String): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)
    val pathEntity = storageFormat.locationUri.map { u => pathToEntity(u.toString) }

    sdEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      storageFormatUniqueAttribute(db, table))
    pathEntity.foreach { e => sdEntity.setAttribute("locationUri", e) }
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("properties", storageFormat.properties.asJava)
    sdEntity.setAttribute("name", storageFormat.toString())

    Seq(Some(sdEntity), pathEntity)
      .filter(_.isDefined)
      .flatten
=======
      table: String,
      isHiveTable: Boolean): Seq[AtlasEntity] = {
    if (isHiveTable) {
      external.hiveStorageDescToEntities(storageFormat, clusterName, db, table)
    } else {
      internal.sparkStorageFormatToEntities(storageFormat, db, table)
    }
>>>>>>> Add Hive support and refactor the code
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

<<<<<<< HEAD
  def processToEntity(qe: QueryExecution,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      inputTables: List[String],
      outputTables: List[String]): AtlasEntity = {
    println(qe.toString())
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      qe.toString() + "." + inputTables.toString() + "." + outputTables.toString())
    entity.setAttribute("name", inputTables.toString() + "." + outputTables.toString())
    entity.setAttribute("inputs", inputs.asJava)
    entity.setAttribute("outputs", outputs.asJava)
    entity.setAttribute("details", qe.toString())
    entity.setAttribute("sparkPlanDescription", qe.sparkPlan.toString())
    entity
  }
=======
  def isHiveTable(tableDefinition: CatalogTable): Boolean =
    tableDefinition.provider.contains("hive")
>>>>>>> Add Hive support and refactor the code

  def tableToEntities(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    if (isHiveTable(tableDefinition)) {
      external.hiveTableToEntities(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntities(tableDefinition, mockDbDefinition)
    }
  }

  def tableUniqueAttribute(db: String, table: String, isHiveTable: Boolean): String = {
    if (isHiveTable) {
      external.hiveTableUniqueAttribute(clusterName, db, table)
    } else {
      internal.sparkTableUniqueAttribute(db, table)
    }
  }

  def MLDirectoryToEntity(
      uri: String,
      directory: String): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_DIRECTORY_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, s"$uri.$directory")
    entity.setAttribute("uri", uri)
    entity.setAttribute("directory", directory)
    entity
  }

  def MLPipelineToEntity(
      pipeline: Pipeline,
      directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_PIPELINE_TYPE_STRING)

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pipeline.uid)
    entity.setAttribute("name", pipeline.uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def MLModelToEntity(
      model: PipelineModel,
      directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_MODEL_TYPE_STRING)

    val uid = model.uid.replaceAll("pipeline", "model")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def MLFitProcessToEntity(
      pipeline: Pipeline,
      pipelineEntity: AtlasEntity,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_FIT_PROCESS_TYPE_STRING)

    val uid = pipeline.uid.replaceAll("pipeline", "fit_process")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("pipeline", pipelineEntity)
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Pipeline entity
    entity.setAttribute("outputs", outputs.asJava)  // ML model entity
    entity
  }

  def MLTransformProcessToEntity(
      model: PipelineModel,
      modelEntity: AtlasEntity,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_TRANSFORM_PROCESS_TYPE_STRING)

    val uid = model.uid.replaceAll("pipeline", "transform_process")
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("model", modelEntity)
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Model entity
    entity.setAttribute("outputs", outputs.asJava)  // Dataset entity
    entity
  }
}
