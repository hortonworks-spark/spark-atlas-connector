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

package com.hortonworks.spark.atlas

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.util.control.NonFatal
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, AtlasTypeUtils, metadata}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

class SparkEntitiesTracker(atlasClientConf: AtlasClientConf)
    extends SparkListener with QueryExecutionListener with Logging {

  case class QueryExecutionDetail(funcName: String, qe: QueryExecution)

  private val capacity = atlasClientConf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  // A blocking queue for Spark Listener ExternalCatalog related events.
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)
  // A blocking queue for Spark SQL QueryExecution details.
  private val queryExecutionQueue = new LinkedBlockingQueue[QueryExecutionDetail](capacity)

  private val timeout = atlasClientConf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  @volatile private var shouldContinue: Boolean = true

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    // We only care SQL related events.
    event match {
      case e: ExternalCatalogEvent =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!shouldContinue) {
      return
    }

    val qeDetail = QueryExecutionDetail(funcName, qe)
    if (!queryExecutionQueue.offer(qeDetail, timeout, TimeUnit.MILLISECONDS)) {
      logError(s"Fail to put query execution $qeDetail into queue within time limit $timeout, " +
        s"will throw it")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    // no-op
  }

  def eventProcess(): Unit = {
    // initialize Atlas client before further processing event.
    if (!initializeAtlasClient()) {
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
            val entity = AtlasEntityUtils.dbToEntity(dbDefinition)
            AtlasEntityUtils.createEntity(entity, AtlasClient.atlasClient())
            logInfo(s"Created db entity $db")

          case DropDatabaseEvent(db) =>
            AtlasEntityUtils.deleteEntity(
              metadata.DB_TYPE_STRING,
              AtlasEntityUtils.dbUniqueAttribute(db),
              AtlasClient.atlasClient())
            logInfo(s"Deleted db entity $db")

          case CreateTableEvent(db, table) =>
            val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)

            val schemaEntities = AtlasEntityUtils.schemaToEntity(tableDefinition.schema, db, table)
            schemaEntities.foreach { entity =>
              AtlasEntityUtils.createEntity(entity, AtlasClient.atlasClient())
            }
            val storageFormatEntity =
              AtlasEntityUtils.storageFormatToEntity(tableDefinition.storage, db, table)

            val dbEntity = AtlasEntityUtils.getEntity(
              metadata.DB_TYPE_STRING,
              AtlasEntityUtils.dbUniqueAttribute(db),
              AtlasClient.atlasClient())

            if (dbEntity.isDefined) {
              val tableEntity = AtlasEntityUtils.tableToEntity(tableDefinition, dbEntity.get,
                schemaEntities, storageFormatEntity)
              AtlasEntityUtils.createEntity(tableEntity, AtlasClient.atlasClient())
              logInfo(s"Created table entity $table")
            } else {
              logWarn(s"Failed to create table entity $table because we cannot find db entity $db")
            }

          case DropTableEvent(db, table) =>
            AtlasEntityUtils.deleteEntity(
              metadata.TABLE_TYPE_STRING,
              AtlasEntityUtils.tableUniqueAttribute(db, table),
              AtlasClient.atlasClient())
            logInfo(s"Deleted table entity $table")

          case RenameTableEvent(db, name, newName) =>
            val tableEntity = AtlasEntityUtils.getEntity(
              metadata.TABLE_TYPE_STRING,
              AtlasEntityUtils.tableUniqueAttribute(db, name),
              AtlasClient.atlasClient())
            if (tableEntity.isDefined) {
              tableEntity.get.setAttribute("table", newName)
              AtlasEntityUtils.updateEntity(
                metadata.TABLE_TYPE_STRING,
                AtlasEntityUtils.tableUniqueAttribute(db, name),
                tableEntity.get,
                AtlasClient.atlasClient())

              logInfo(s"Rename table entity $name to $newName")
            }
        }

        Option(queryExecutionQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach {
          case QueryExecutionDetail(funcName, qe) =>

        }
      } catch {
        case _: InterruptedException =>
          logDebug(s"Thread is interrupted")
          stopped = false
      }
    }
  }

  private def initializeAtlasClient(): Boolean = {
    try {
      // initialize atlasClient
      val atlasClient = AtlasClient.atlasClient()

      // try to create all the types if not
      AtlasTypeUtils.checkAndCreateTypes(atlasClient)
      true
    } catch {
      case NonFatal(e) =>
        logError(s"Fail to initialize Atlas client, stop this listener", e)
        true
    }
  }

  private def analyzeQueryExecution(func: String, qe: QueryExecution): Unit = {
//    val inputNodes = new ArrayBuffer[AtlasEntity]()
//    qe.analyzed.foreach {
//    }
  }
}
