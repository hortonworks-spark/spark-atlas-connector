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

import scala.collection.JavaConverters._

import org.apache.atlas.AtlasClient
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.types.StructType

import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object internal extends Logging {

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkDbToEntities(dbDefinition: CatalogDatabase): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)
    val pathEntity = external.pathToEntity(dbDefinition.locationUri.toString)

    dbEntity.setAttribute(
      AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, sparkDbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("locationUri", pathEntity)
    dbEntity.setAttribute("properties", dbDefinition.properties.asJava)
    Seq(dbEntity, pathEntity)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkStorageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)
    val pathEntity = storageFormat.locationUri.map { u => external.pathToEntity(u.toString) }

    sdEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      sparkStorageFormatUniqueAttribute(db, table))
    pathEntity.foreach { e => sdEntity.setAttribute("locationUri", e) }
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("properties", storageFormat.properties.asJava)

    Seq(Some(sdEntity), pathEntity)
      .filter(_.isDefined)
      .flatten
  }

  def sparkColumnUniqueAttribute(db: String, table: String, col: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.col-$col"
  }

  def sparkSchemaToEntities(schema: StructType, db: String, table: String): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(metadata.COLUMN_TYPE_STRING)

      entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        sparkColumnUniqueAttribute(db, table, struct.name))
      entity.setAttribute("name", struct.name)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("nullable", struct.nullable)
      entity.setAttribute("metadata", struct.metadata.toString())
      entity
    }.toList
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def sparkTableToEntities(
      tableDefinition: CatalogTable,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val db = tableDefinition.identifier.database.getOrElse("default")
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = sparkDbToEntities(dbDefinition)
    val sdEntities =
      sparkStorageFormatToEntities(tableDefinition.storage, db, tableDefinition.identifier.table)
    val schemaEntities =
      sparkSchemaToEntities(tableDefinition.schema, db, tableDefinition.identifier.table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      sparkTableUniqueAttribute(db, tableDefinition.identifier.table))
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

  def sparkProcessUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

  def sparkProcessToEntity(
     currUser: String,
     remoteUser: String,
     sqlExecutionStart: SparkListenerSQLExecutionStart,
     sqlExecutionEnd: SparkListenerSQLExecutionEnd,
     inputs: List[AtlasEntity],
     outputs: List[AtlasEntity]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)

    entity.setAttribute("currUser", currUser)
    entity.setAttribute("remoteUser", remoteUser)
    entity.setAttribute("executionId", sqlExecutionStart.executionId)
    entity.setAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      sparkProcessUniqueAttribute(sqlExecutionStart.executionId))
    entity.setAttribute("startTime", sqlExecutionStart.time)
    entity.setAttribute("endTime", sqlExecutionEnd.time)
    entity.setAttribute("description", sqlExecutionStart.description)
    entity.setAttribute("details", sqlExecutionStart.details)
    entity.setAttribute("physicalPlanDescription", sqlExecutionStart.physicalPlanDescription)
    entity.setAttribute("inputs", inputs.asJava)
    entity.setAttribute("outputs", outputs.asJava)
    entity
  }


}
