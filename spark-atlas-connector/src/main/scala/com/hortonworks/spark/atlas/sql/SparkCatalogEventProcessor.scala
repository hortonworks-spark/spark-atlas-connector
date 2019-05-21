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

package com.hortonworks.spark.atlas.sql

import scala.collection.mutable
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog._
import com.hortonworks.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf, AtlasEntityReadHelper}
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

class SparkCatalogEventProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with AtlasEntityUtils with Logging {

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  override protected def process(e: ExternalCatalogEvent): Unit = {
    if (SparkUtils.usingRemoteMetastoreService()) {
      // SAC will not handle any DDL events when remote HMS is used:
      // Hive hook will take care of all DDL events in Hive Metastore Service.
      // No-op here.
      return
    }

    e match {
      case CreateDatabasePreEvent(_) => // No-op

      case CreateDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val entity = sparkDbToEntity(dbDefinition)
        atlasClient.createEntitiesWithDependencies(entity)
        logDebug(s"Created db entity $db")

      case DropDatabasePreEvent(db) =>
        try {
          cachedObject.put(sparkDbUniqueAttribute(db),
            SparkUtils.getExternalCatalog().getDatabase(db))
        } catch {
          case _: NoSuchDatabaseException =>
            logDebug(s"Spark already deleted the database: $db")
        }

      case DropDatabaseEvent(db) =>
        atlasClient.deleteEntityWithUniqueAttr(sparkDbType, sparkDbUniqueAttribute(db))

        cachedObject.remove(sparkDbUniqueAttribute(db)).foreach { o =>
          val dbDef = o.asInstanceOf[CatalogDatabase]
          val path = dbDef.locationUri.toString
          val pathEntity = external.pathToEntity(path)

          atlasClient.deleteEntityWithUniqueAttr(pathEntity.entity.getTypeName,
            AtlasEntityReadHelper.getQualifiedName(pathEntity.entity))
        }

        logDebug(s"Deleted db entity $db")

      case CreateTablePreEvent(_, _) => // No-op

      // TODO. We should also not create/alter view table in Atlas
      case CreateTableEvent(db, table) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        val tableEntity = sparkTableToEntity(tableDefinition)
        atlasClient.createEntitiesWithDependencies(tableEntity)
        logDebug(s"Created table entity $table without columns")

      case DropTablePreEvent(_, _) => // No-op

      case DropTableEvent(db, table) =>
        logDebug(s"Can't handle drop table event since we don't have context information for " +
          s"table $table in db $db. Can't delete table entity and corresponding entities.")

      case RenameTableEvent(db, name, newName) =>
        // Update storageFormat's unique attribute
        val sdEntity = new AtlasEntity(sparkStorageFormatType)
        sdEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          sparkStorageFormatUniqueAttribute(db, newName))
        atlasClient.updateEntityWithUniqueAttr(
          sparkStorageFormatType,
          sparkStorageFormatUniqueAttribute(db, name),
          sdEntity)

        // Update Table name and Table's unique attribute
        val tableEntity = new AtlasEntity(sparkTableType)
        tableEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          sparkTableUniqueAttribute(db, newName))
        tableEntity.setAttribute("name", newName)
        atlasClient.updateEntityWithUniqueAttr(
          sparkTableType,
          sparkTableUniqueAttribute(db, name),
          tableEntity)

        logDebug(s"Rename table entity $name to $newName")

      case AlterDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val dbEntity = sparkDbToEntity(dbDefinition)
        atlasClient.createEntitiesWithDependencies(dbEntity)
        logDebug(s"Updated DB properties")

      case AlterTableEvent(db, table, kind) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        kind match {
          case "table" =>
            val tableEntity = sparkTableToEntityForAlterTable(tableDefinition)
            atlasClient.createEntitiesWithDependencies(tableEntity)
            logDebug(s"Updated table entity $table without columns")

          case "dataSchema" =>
            // We don't mind updating column
            logDebug("Detected updating of table schema but ignored: " +
              "column update will not be tracked here")

          case "stats" =>
            logDebug(s"Stats update will not be tracked here")

          case _ =>
          // No op.
        }

      case f =>
        logDebug(s"Drop unknown event $f")
    }
  }
}
