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

import java.util.Date

import com.hortonworks.spark.atlas.AtlasUtils

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object internal extends Logging {

  val cachedObjects = new mutable.HashMap[String, Object]

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkDbToEntities(dbDefinition: CatalogDatabase, cluster: String, owner: String)
    : Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)

    dbEntity.setAttribute(
      "qualifiedName", sparkDbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    dbEntity.setAttribute("ownerType", "USER")
    Seq(dbEntity)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkStorageFormatToEntities(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute("qualifiedName",
      sparkStorageFormatUniqueAttribute(db, table))
    storageFormat.locationUri.foreach(uri => sdEntity.setAttribute("location", uri.toString))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)

    Seq(sdEntity)
  }

  def sparkColumnUniqueAttribute(db: String, table: String, col: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.col-$col"
  }

  def sparkColumnToEntities(schema: StructType, db: String, table: String): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(metadata.COLUMN_TYPE_STRING)

      entity.setAttribute("qualifiedName",
        sparkColumnUniqueAttribute(db, table, struct.name.toLowerCase))
      entity.setAttribute("name", struct.name.toLowerCase)
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
      tblDefinition: CatalogTable,
      clusterName: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = tableDefinition.identifier.database.getOrElse("default")
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = sparkDbToEntities(dbDefinition, clusterName, tableDefinition.owner)
    val sdEntities =
      sparkStorageFormatToEntities(tableDefinition.storage, db, tableDefinition.identifier.table)
    val schemaEntities =
      sparkColumnToEntities(tableDefinition.schema, db, tableDefinition.identifier.table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute("qualifiedName",
      sparkTableUniqueAttribute(db, tableDefinition.identifier.table))
    tblEntity.setAttribute("name", tableDefinition.identifier.table)
    tblEntity.setAttribute("db", dbEntities.head)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("sd", sdEntities.head)
    tblEntity.setAttribute("columns", schemaEntities.asJava)
    tableDefinition.provider.foreach(tblEntity.setAttribute("provider", _))
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities
  }

  def sparkTableToEntitiesForAlterTable(
      tblDefinition: CatalogTable,
      clusterName: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val typesToPick = Seq(metadata.TABLE_TYPE_STRING, metadata.COLUMN_TYPE_STRING)
    val entities = sparkTableToEntities(tblDefinition, clusterName, mockDbDefinition)

    val dbEntity = entities.filter(e => e.getTypeName.equals(metadata.DB_TYPE_STRING)).head
    val sdEntity = entities.filter(e => e.getTypeName.equals(metadata.STORAGEDESC_TYPE_STRING)).head
    val tableEntity = entities.filter(e => e.getTypeName.equals(metadata.TABLE_TYPE_STRING)).head

    // override attribute with reference - Atlas should already have these entities
    tableEntity.setAttribute("db", AtlasUtils.entityToReference(dbEntity, useGuid = false))
    tableEntity.setAttribute("sd", AtlasUtils.entityToReference(sdEntity, useGuid = false))

    entities.filter(e => typesToPick.contains(e.getTypeName))
  }

  def sparkProcessUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

  def sparkProcessToEntity(
      qe: QueryExecution,
      executionId: Long,
      executionTime: Long,
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      query: Option[String] = None): AtlasEntity = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)
    val name = query.getOrElse(sparkProcessUniqueAttribute(executionId))

    entity.setAttribute(
      "qualifiedName", sparkProcessUniqueAttribute(executionId))
    entity.setAttribute("name", name)
    entity.setAttribute("executionId", executionId)
    entity.setAttribute("currUser", SparkUtils.currUser())
    entity.setAttribute("remoteUser", SparkUtils.currSessionUser(qe))
    entity.setAttribute("inputs", inputs.asJava)
    entity.setAttribute("outputs", outputs.asJava)
    entity.setAttribute("executionTime", executionTime)
    entity.setAttribute("details", qe.toString())
    entity.setAttribute("sparkPlanDescription", qe.sparkPlan.toString())
    entity
  }

  // ================ ML related entities ==================
  def mlDirectoryToEntity(uri: String, directory: String): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_DIRECTORY_TYPE_STRING)

    entity.setAttribute("qualifiedName", s"$uri.$directory")
    entity.setAttribute("name", s"$uri.$directory")
    entity.setAttribute("uri", uri)
    entity.setAttribute("directory", directory)
    entity
  }

  def mlPipelineToEntity(pipeline_uid: String, directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_PIPELINE_TYPE_STRING)

    entity.setAttribute("qualifiedName", pipeline_uid)
    entity.setAttribute("name", pipeline_uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def mlModelToEntity(model_uid: String, directory: AtlasEntity): AtlasEntity = {
    val entity = new AtlasEntity(metadata.ML_MODEL_TYPE_STRING)

    val uid = model_uid.replaceAll("pipeline", "model")
    entity.setAttribute("qualifiedName", uid)
    entity.setAttribute("name", uid)
    entity.setAttribute("directory", directory)
    entity
  }

  def etlProcessToEntity(
      inputs: List[AtlasEntity],
      outputs: List[AtlasEntity],
      logMap: Map[String, String]): AtlasEntity = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)

    val appId = SparkUtils.sparkSession.sparkContext.applicationId
    val appName = SparkUtils.sparkSession.sparkContext.appName match {
      case "Spark shell" => s"Spark Job + $appId"
      case default => default + s" $appId"
    }
    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkUtils.currUser())
    entity.setAttribute("inputs", inputs.asJava)  // Dataset and Model entity
    entity.setAttribute("outputs", outputs.asJava)  // Dataset entity
    logMap.foreach { case (k, v) => entity.setAttribute(k, v)}
    entity
  }

  def updateMLProcessToEntity(
      inputs: Seq[AtlasEntity],
      outputs: Seq[AtlasEntity],
      logMap: Map[String, String]): Seq[AtlasEntity] = {

    val model_uid = internal.cachedObjects("model_uid").asInstanceOf[String]
    val modelEntity = internal.cachedObjects(s"${model_uid}_modelEntity").
      asInstanceOf[AtlasEntity]
    val modelDirEntity = internal.cachedObjects(s"${model_uid}_modelDirEntity").
      asInstanceOf[AtlasEntity]

    if (internal.cachedObjects.contains("fit_process")) {

      // spark ml fit process
      val processEntity = internal.etlProcessToEntity(
        List(inputs.head), List(outputs.head), logMap)

      (Seq(processEntity, modelDirEntity, modelEntity)
        ++ inputs ++ outputs)
    } else {
      val new_inputs = List(inputs.head, modelDirEntity, modelEntity)

      // spark ml fit and score process
      val processEntity = internal.etlProcessToEntity(
        new_inputs, List(outputs.head), logMap)

      (Seq(processEntity, modelDirEntity, modelEntity)
        ++ new_inputs ++ outputs)
    }
  }
}
