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

import com.hortonworks.spark.atlas.{AtlasEntityWithDependencies, AtlasReferenceable}

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object internal extends Logging {

  val cachedObjects = new mutable.HashMap[String, Object]

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkDbToEntity(
      dbDefinition: CatalogDatabase,
      cluster: String,
      owner: String): AtlasEntityWithDependencies = {
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

    AtlasEntityWithDependencies(dbEntity)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkStorageFormatToEntity(
      storageFormat: CatalogStorageFormat,
      db: String,
      table: String): AtlasEntityWithDependencies = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute("qualifiedName",
      sparkStorageFormatUniqueAttribute(db, table))
    storageFormat.locationUri.foreach(uri => sdEntity.setAttribute("location", uri.toString))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)

    AtlasEntityWithDependencies(sdEntity)
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def sparkTableToEntity(
      tblDefinition: CatalogTable,
      clusterName: String,
      mockDbDefinition: Option[CatalogDatabase] = None): AtlasEntityWithDependencies = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntity = sparkDbToEntity(dbDefinition, clusterName, tableDefinition.owner)
    val sdEntity =
      sparkStorageFormatToEntity(tableDefinition.storage, db, table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute("qualifiedName",
      sparkTableUniqueAttribute(db, table))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("schemaDesc", tableDefinition.schema.simpleString)
    tblEntity.setAttribute("provider", tableDefinition.provider.getOrElse(""))
    if (tableDefinition.tracksPartitionsInCatalog) {
      tblEntity.setAttribute("partitionProvider", "Catalog")
    }
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    tblEntity.setRelationshipAttribute("db", dbEntity.asObjectId)
    tblEntity.setRelationshipAttribute("sd", sdEntity.asObjectId)

    new AtlasEntityWithDependencies(tblEntity, Seq(dbEntity, sdEntity))
  }

  def sparkTableToEntityForAlterTable(
      tblDefinition: CatalogTable,
      clusterName: String,
      mockDbDefinition: Option[CatalogDatabase] = None): AtlasReferenceable = {
    val tableEntity = sparkTableToEntity(tblDefinition, clusterName, mockDbDefinition)
    val deps = tableEntity.dependencies

    val dbEntity = deps.filter(_.typeName == metadata.DB_TYPE_STRING).head
    val sdEntity = deps.filter(_.typeName == metadata.STORAGEDESC_TYPE_STRING).head

    // override attribute with reference - Atlas should already have these entities
    tableEntity.entity.setRelationshipAttribute("db", dbEntity.asObjectId)
    tableEntity.entity.setRelationshipAttribute("sd", sdEntity.asObjectId)

    AtlasEntityWithDependencies(tableEntity.entity)
  }

  def sparkProcessUniqueAttribute(executionId: Long): String = {
    SparkUtils.sparkSession.sparkContext.applicationId + "." + executionId
  }

  // ================ ML related entities ==================
  def mlDirectoryToEntity(uri: String, directory: String): AtlasEntityWithDependencies = {
    val entity = new AtlasEntity(metadata.ML_DIRECTORY_TYPE_STRING)

    entity.setAttribute("qualifiedName", s"$uri.$directory")
    entity.setAttribute("name", s"$uri.$directory")
    entity.setAttribute("uri", uri)
    entity.setAttribute("directory", directory)

    AtlasEntityWithDependencies(entity)
  }

  def mlPipelineToEntity(
      pipeline_uid: String,
      directory: AtlasEntityWithDependencies): AtlasEntityWithDependencies = {
    val entity = new AtlasEntity(metadata.ML_PIPELINE_TYPE_STRING)

    entity.setAttribute("qualifiedName", pipeline_uid)
    entity.setAttribute("name", pipeline_uid)
    entity.setRelationshipAttribute("directory", directory.asObjectId)

    new AtlasEntityWithDependencies(entity, Seq(directory))
  }

  def mlModelToEntity(
      model_uid: String,
      directory: AtlasEntityWithDependencies): AtlasEntityWithDependencies = {
    val entity = new AtlasEntity(metadata.ML_MODEL_TYPE_STRING)

    val uid = model_uid.replaceAll("pipeline", "model")
    entity.setAttribute("qualifiedName", uid)
    entity.setAttribute("name", uid)
    entity.setRelationshipAttribute("directory", directory.asObjectId)

    new AtlasEntityWithDependencies(entity, Seq(directory))
  }

  def etlProcessToEntity(
      inputs: Seq[AtlasReferenceable],
      outputs: Seq[AtlasReferenceable],
      logMap: Map[String, String]): AtlasEntityWithDependencies = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)

    val appId = SparkUtils.sparkSession.sparkContext.applicationId
    val appName = SparkUtils.sparkSession.sparkContext.appName match {
      case "Spark shell" => s"Spark Job + $appId"
      case default => default + s" $appId"
    }
    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkUtils.currUser())

    val inputObjIds = inputs.map(_.asObjectId).asJava
    val outputObjIds = outputs.map(_.asObjectId).asJava

    entity.setAttribute("inputs", inputObjIds)  // Dataset and Model entity
    entity.setAttribute("outputs", outputObjIds)  // Dataset entity
    logMap.foreach { case (k, v) => entity.setAttribute(k, v)}

    new AtlasEntityWithDependencies(entity, inputs ++ outputs)
  }

  def updateMLProcessToEntity(
      inputs: Seq[AtlasReferenceable],
      outputs: Seq[AtlasReferenceable],
      logMap: Map[String, String]): AtlasEntityWithDependencies = {

    val model_uid = internal.cachedObjects("model_uid").asInstanceOf[String]
    val modelEntity = internal.cachedObjects(s"${model_uid}_modelEntity").
      asInstanceOf[AtlasEntityWithDependencies]
    val modelDirEntity = internal.cachedObjects(s"${model_uid}_modelDirEntity").
      asInstanceOf[AtlasEntityWithDependencies]

    if (internal.cachedObjects.contains("fit_process")) {

      // spark ml fit process
      val processEntity = internal.etlProcessToEntity(
        List(inputs.head), List(outputs.head), logMap)

      processEntity.dependenciesAdded(Seq(modelDirEntity, modelEntity))
    } else {
      val new_inputs = List(inputs.head, modelDirEntity, modelEntity)

      // spark ml fit and score process
      val processEntity = internal.etlProcessToEntity(
        new_inputs, List(outputs.head), logMap)

      processEntity.dependenciesAdded(Seq(modelDirEntity, modelEntity))
    }
  }
}
