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


import java.io.File
import java.net.{URI, URISyntaxException}

import scala.collection.JavaConverters._

import org.apache.atlas.{AtlasClient, AtlasConstants}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType

import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object AtlasEntityUtils extends Logging {

  def dbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def dbToEntities(dbDefinition: CatalogDatabase): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)
    val pathEntity = pathToEntity(dbDefinition.locationUri.toString)

    dbEntity.setAttribute(
      AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("locationUri", pathEntity)
    dbEntity.setAttribute("properties", dbDefinition.properties.asJava)
    Seq(dbEntity, pathEntity)
  }

  def storageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def storageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
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
  }

  def columnUniqueAttribute(db: String, table: String, col: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.col-$col"
  }

  def schemaToEntities(schema: StructType, db: String, table: String): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(metadata.COLUMN_TYPE_STRING)

      entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        columnUniqueAttribute(db, table, struct.name))
      entity.setAttribute("name", struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("nullable", struct.nullable)
      entity.setAttribute("metadata", struct.metadata.toString())
      entity
    }.toList
  }

  def tableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def tableToEntities(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val db = tableDefinition.identifier.database.getOrElse("default")
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = dbToEntities(dbDefinition)
    val sdEntities =
      storageFormatToEntities(tableDefinition.storage, db, tableDefinition.identifier.table)
    val schemaEntities =
      schemaToEntities(tableDefinition.schema, db, tableDefinition.identifier.table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      tableUniqueAttribute(db, tableDefinition.identifier.table))
    tblEntity.setAttribute("name", tableDefinition.identifier.table)
    tblEntity.setAttribute("database", dbEntities.head)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("storage", sdEntities.head)
    tblEntity.setAttribute("schema", schemaEntities.asJava)
    tableDefinition.provider.foreach(tblEntity.setAttribute("provider", _))
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("createTime", tableDefinition.createTime)
    tblEntity.setAttribute("lastAccessTime", tableDefinition.lastAccessTime)
    tblEntity.setAttribute("properties", tableDefinition.properties.asJava)
    tableDefinition.viewText.foreach(tblEntity.setAttribute("viewText", _))
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities
  }

  def processUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

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

  def pathToEntity(path: String): AtlasEntity = {
    val uri = resolveURI(path)
    val entity = if (uri.getScheme == "hfds") {
      new AtlasEntity(metadata.HDFS_PATH_TYPE_STRING)
    } else {
      new AtlasEntity(metadata.FS_PATH_TYPE_STRING)
    }

    val fsPath = new Path(uri)
    entity.setAttribute(AtlasClient.NAME,
      Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
    entity.setAttribute("path", Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uri.toString)
    if (uri.getScheme == "hdfs") {
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)
    }

    entity
  }

  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
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

    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, model.uid)
    entity.setAttribute("name", model.uid)
    entity.setAttribute("pid", model.parent.uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def MLFitProcessToEntity(
      pipeline: Pipeline,
      directory: AtlasEntity,
      startTime: Long,
      endTime: Long,
      input: AtlasEntity,
      output: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_FIT_PROCESS_TYPE_STRING)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pipeline.uid)
    entity.setAttribute("name", pipeline.uid)
    entity.setAttribute("directory", directory)
    entity.setAttribute("startTime", startTime)
    entity.setAttribute("endTime", endTime)
    entity.setAttribute("inputs", List(input).asJava)  // Dataset entity
    entity.setAttribute("outputs", List(output).asJava)  // ML model entity
    entity
  }

  def MLTransformProcessToEntity(
      pipelineModel: PipelineModel,
      directory: AtlasEntity,
      startTime: Long,
      endTime: Long,
      input: AtlasEntity,
      output: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_TRANSFORM_PROCESS_TYPE_STRING)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pipelineModel.uid)
    entity.setAttribute("name", pipelineModel.uid)
    entity.setAttribute("directory", directory)
    entity.setAttribute("startTime", startTime)
    entity.setAttribute("endTime", endTime)
    entity.setAttribute("inputs", List(input).asJava)  // Dataset entity
    entity.setAttribute("outputs", List(output).asJava)  // Dataset entity
    entity
  }
}
