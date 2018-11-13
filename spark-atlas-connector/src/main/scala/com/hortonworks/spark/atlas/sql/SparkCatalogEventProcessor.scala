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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._

import com.hortonworks.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf}
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

class SparkCatalogEventProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with AtlasEntityUtils with Logging {

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  override protected def process(e: ExternalCatalogEvent): Unit = {
    e match {
      case CreateDatabasePreEvent(_) => // No-op

      case CreateDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val entities = dbToEntities(dbDefinition)
        atlasClient.createEntities(entities)
        logInfo(s"Created db entity $db")

      case DropDatabasePreEvent(db) =>
        try {
          cachedObject.put(dbUniqueAttribute(db), SparkUtils.getExternalCatalog().getDatabase(db))
        } catch {
          case _: NoSuchDatabaseException =>
            logInfo(s"Spark already deleted the database: $db")
        }

      case DropDatabaseEvent(db) =>
        atlasClient.deleteEntityWithUniqueAttr(dbType, dbUniqueAttribute(db))

        cachedObject.remove(dbUniqueAttribute(db)).foreach { o =>
          val dbDef = o.asInstanceOf[CatalogDatabase]
          val path = dbDef.locationUri.toString
          val pathEntity = external.pathToEntity(path)
          atlasClient.deleteEntityWithUniqueAttr(pathEntity.getTypeName, path)
        }

        logInfo(s"Deleted db entity $db")

      case CreateTablePreEvent(_, _) => // No-op

      // TODO. We should also not create/alter view table in Atlas
      case CreateTableEvent(db, table) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        val tableEntities = tableToEntities(tableDefinition)
        atlasClient.createEntities(tableEntities)
        logInfo(s"Created table entity $table")

      case DropTablePreEvent(db, table) =>
        try {
          val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
          cachedObject.put(
            tableUniqueAttribute(db, table, isHiveTable(tableDefinition)), tableDefinition)
        } catch {
          case _: NoSuchTableException =>
            logInfo(s"Spark already deleted the table: $db.$table")
        }

      case DropTableEvent(db, table) =>
        // Delete table
        atlasClient.deleteEntityWithUniqueAttr(
          tableType(true),
          tableUniqueAttribute(db, table, true))
        logInfo(s"Deleted table entity $table")

        // Try to delete the related entities from Spark-side
        cachedObject.remove(tableUniqueAttribute(db, table, isHiveTable = true))
          .orElse(cachedObject.remove(tableUniqueAttribute(db, table, isHiveTable = false)))
          .foreach { o =>
            val tblDef = o.asInstanceOf[CatalogTable]
            val isHiveTbl = isHiveTable(tblDef)

            atlasClient.deleteEntityWithUniqueAttr(
              storageFormatType(isHiveTbl), storageFormatUniqueAttribute(db, table, isHiveTbl))
            logInfo(s"Deleted storage entity for $db.$table")
            tblDef.schema.foreach { f =>
              atlasClient.deleteEntityWithUniqueAttr(
                columnType(isHiveTbl), columnUniqueAttribute(db, table, f.name, isHiveTbl))
              logInfo(s"Deleted column entity $db.$table.${f.name}")
            }

          }

      case RenameTableEvent(db, name, newName) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, newName)
        val isHiveTbl = isHiveTable(tableDefinition)

        // Update storageFormat's unique attribute
        val sdEntity = new AtlasEntity(storageFormatType(isHiveTbl))
        sdEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          storageFormatUniqueAttribute(db, newName, isHiveTbl))
        atlasClient.updateEntityWithUniqueAttr(
          storageFormatType(isHiveTbl),
          storageFormatUniqueAttribute(db, name, isHiveTbl),
          sdEntity)

        // Update column's unique attribute
        tableDefinition.schema.foreach { sf =>
          val colEntity = new AtlasEntity(columnType(isHiveTbl))
          colEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            columnUniqueAttribute(db, newName, sf.name, isHiveTbl))
          atlasClient.updateEntityWithUniqueAttr(
            columnType(isHiveTbl),
            columnUniqueAttribute(db, name, sf.name, isHiveTbl),
            colEntity)
        }

        // Update Table name and Table's unique attribute
        val tableEntity = new AtlasEntity(tableType(isHiveTbl))
        tableEntity.setAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
          tableUniqueAttribute(db, newName, isHiveTbl))
        tableEntity.setAttribute("name", newName)
        atlasClient.updateEntityWithUniqueAttr(
          tableType(isHiveTbl),
          tableUniqueAttribute(db, name, isHiveTbl),
          tableEntity)

        logInfo(s"Rename table entity $name to $newName")

      case AlterDatabaseEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val dbEntities = dbToEntities(dbDefinition)
        atlasClient.createEntities(dbEntities)
        logInfo(s"Updated DB properties")

      case AlterTableEvent(db, table, kind) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        kind match {
          case "table" =>
            val tableEntities = tableToEntities(tableDefinition)
            atlasClient.createEntities(tableEntities)
            logInfo(s"Updated table entity $table")

          case "dataSchema" =>
            val isHiveTbl = isHiveTable(tableDefinition)
            val schemaEntities =
              schemaToEntities(tableDefinition.schema, db, table, isHiveTbl)
            atlasClient.createEntities(schemaEntities)

            val tableEntity = new AtlasEntity(tableType(isHiveTbl))
            tableEntity.setAttribute("spark_schema", schemaEntities.asJava)
            atlasClient.updateEntityWithUniqueAttr(
              tableType(isHiveTbl),
              tableUniqueAttribute(db, table, isHiveTbl),
              tableEntity)
            logInfo(s"Updated table schema")

          case "stats" =>
            logDebug(s"Stats update will not be tracked here")

          case _ =>
          // No op.
        }

      case f =>
        logInfo(s"Drop unknown event $f")
    }
  }
}
