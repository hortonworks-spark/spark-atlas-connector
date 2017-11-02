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

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CreateDatabaseEvent, ExternalCatalog, ExternalCatalogEvent}

import com.hortonworks.spark.atlas.types.AtlasEntityUtil
import com.hortonworks.spark.atlas.utils.Logging

class SparkEntitiesTracker(atlasClientConf: AtlasClientConf) extends SparkListener with Logging {
  private val capacity = atlasClientConf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)

  private val timeout = atlasClientConf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  private lazy val externalCatalog = getExternalCatalog()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: ExternalCatalogEvent =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue, will throw it")
        }
    }
  }

  def eventProcess(): Unit = {
    Option(eventQueue.poll()).foreach {
      case CreateDatabaseEvent(db) =>
        val dbDefinition = externalCatalog.getDatabase(db)
        val entity = AtlasEntityUtil.dbToEntity(dbDefinition)
        ???
    }
  }

  private def getExternalCatalog(): ExternalCatalog = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (session.isEmpty) {
      throw new IllegalStateException("Cannot find active or default SparkSession in the current" +
        " context")
    }

    val catalog = session.get.sharedState.externalCatalog
    require(catalog != null, "catalog is null")
    catalog
  }
}
