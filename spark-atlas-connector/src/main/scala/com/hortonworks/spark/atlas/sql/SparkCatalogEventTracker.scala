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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog._

import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, RestAtlasClient}
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, SparkAtlasModel, external}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

class SparkCatalogEventTracker(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends SparkListener with AbstractService with AtlasEntityUtils with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  // A blocking queue for Spark Listener ExternalCatalog related events.
  @VisibleForTesting
  private[atlas] val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)

  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  @VisibleForTesting
  @volatile private[atlas] var shouldContinue: Boolean = true

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    // We only care about SQL related events.
    event match {
      case e: ExternalCatalogEvent =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }

      case _ =>
        // Ignore other events
    }
  }

  @VisibleForTesting
  protected[atlas] override def eventProcess(): Unit = {
    // initialize Atlas client before further processing event.
    if (!initializeSparkModel()) {
      logError("Fail to initialize Atlas Client, will discard all the received events and stop " +
        "working")

      shouldContinue = false
      eventQueue.clear()
      return
    }

    var stopped = false
    while (!stopped) {
      try {
        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach {
          case CreateDatabaseEvent(db) =>
            val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
            val entities = dbToEntities(dbDefinition)
            atlasClient.createEntities(entities)
            logInfo(s"Created db entity $db")

          case DropDatabasePreEvent(db) =>
            cachedObject.put(dbUniqueAttribute(db), SparkUtils.getExternalCatalog().getDatabase(db))

          case DropDatabaseEvent(db) =>
            atlasClient.deleteEntityWithUniqueAttr(dbType, dbUniqueAttribute(db))

            cachedObject.remove(dbUniqueAttribute(db)).foreach { o =>
              val dbDef = o.asInstanceOf[CatalogDatabase]
              val path = dbDef.locationUri.toString
              val pathEntity = external.pathToEntity(path)
              atlasClient.deleteEntityWithUniqueAttr(pathEntity.getTypeName, path)
            }

            logInfo(s"Deleted db entity $db")

          // TODO. We should also not create/alter view table in Atlas
          case CreateTableEvent(db, table) =>
            val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
            val tableEntities = tableToEntities(tableDefinition)
            atlasClient.createEntities(tableEntities)
            logInfo(s"Created table entity $table")

          case DropTablePreEvent(db, table) =>
            val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
            cachedObject.put(
              tableUniqueAttribute(db, table, isHiveTable(tableDefinition)), tableDefinition)

          case DropTableEvent(db, table) =>
            cachedObject.remove(tableUniqueAttribute(db, table, isHiveTable = true))
              .orElse(cachedObject.remove(tableUniqueAttribute(db, table, isHiveTable = false)))
              .foreach { o =>
                val tblDef = o.asInstanceOf[CatalogTable]
                val isHiveTbl = isHiveTable(tblDef)

                atlasClient.deleteEntityWithUniqueAttr(
                  tableType(isHiveTbl), tableUniqueAttribute(db, table, isHiveTbl))
                atlasClient.deleteEntityWithUniqueAttr(
                  storageFormatType(isHiveTbl), storageFormatUniqueAttribute(db, table, isHiveTbl))
                tblDef.schema.foreach { f =>
                  atlasClient.deleteEntityWithUniqueAttr(
                    columnType(isHiveTbl), columnUniqueAttribute(db, table, f.name, isHiveTbl))
                }
              }
            logInfo(s"Deleted table entity $table")

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

          case e if e.getClass.getName ==
            "org.apache.spark.sql.catalyst.catalog.AlterDatabaseEvent" =>
            try {
              val dbName = e.getClass.getMethod("database").invoke(e).asInstanceOf[String]
              val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(dbName)
              val dbEntities = dbToEntities(dbDefinition)
              atlasClient.createEntities(dbEntities)
              logInfo(s"Updated DB properties")
            } catch {
              case NonFatal(t) => logWarn(s"Failed to update DB properties", t)
            }

          case e if e.getClass.getName == "org.apache.spark.sql.catalyst.catalog.AlterTableEvent" =>
            try {
              val dbName = e.getClass.getMethod("database").invoke(e).asInstanceOf[String]
              val tableName = e.getClass.getMethod("name").invoke(e).asInstanceOf[String]
              val kind = e.getClass.getMethod("kind").invoke(e).asInstanceOf[String]

              val tableDefinition = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
              kind match {
                case "table" =>
                  val tableEntities = tableToEntities(tableDefinition)
                  atlasClient.createEntities(tableEntities)
                  logInfo(s"Updated table entity $tableName")

                case "dataSchema" =>
                  val isHiveTbl = isHiveTable(tableDefinition)
                  val schemaEntities =
                    schemaToEntities(tableDefinition.schema, dbName, tableName, isHiveTbl)
                  atlasClient.createEntities(schemaEntities)

                  val tableEntity = new AtlasEntity(tableType(isHiveTbl))
                  tableEntity.setAttribute("schema", schemaEntities.asJava)
                  atlasClient.updateEntityWithUniqueAttr(
                    tableType(isHiveTbl),
                    tableUniqueAttribute(dbName, tableName, isHiveTbl),
                    tableEntity)
                  logInfo(s"Updated table schema")

                case "stats" =>
                  logDebug(s"Stats update will not be tracked here")

                case _ =>
                  // No op.
              }
            } catch {
              case NonFatal(t) =>
                logWarn("Failed to update table entity", t)
            }

          case e =>
            logInfo(s"Drop unknown event $e")
        }
      } catch {
        case _: InterruptedException =>
          logDebug(s"Thread is interrupted")
          stopped = true

        case NonFatal(e) =>
          logWarn(s" Caught exception during parsing catalog event", e)
      }
    }
  }

  private def initializeSparkModel(): Boolean = {
    try {
      val checkModelInStart = conf.get(AtlasClientConf.CHECK_MODEL_IN_START).toBoolean
      if (checkModelInStart) {
        val restClient = if (!atlasClient.isInstanceOf[RestAtlasClient]) {
          logWarn("Spark Atlas Model check and creation can only work with REST client, so " +
            "creating a new REST client")
          new RestAtlasClient(conf)
        } else {
          atlasClient
        }

        SparkAtlasModel.checkAndCreateTypes(restClient)
      }

      true
    } catch {
      case NonFatal(e) =>
        logError(s"Fail to initialize Atlas client, stop this listener", e)
        false
    }
  }
}
