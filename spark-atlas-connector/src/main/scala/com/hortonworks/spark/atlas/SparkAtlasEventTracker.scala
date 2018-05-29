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

import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import com.hortonworks.spark.atlas.sql._
import com.hortonworks.spark.atlas.ml.MLPipelineEventProcessor
import com.hortonworks.spark.atlas.types.SparkAtlasModel
import com.hortonworks.spark.atlas.utils.Logging

class SparkAtlasEventTracker(atlasClient: AtlasClient, atlasClientConf: AtlasClientConf)
    extends SparkListener with QueryExecutionListener with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private var shouldContinue: Boolean = true

  if (!initializeSparkModel()) {
    shouldContinue = false
  }

  // Processor to handle DDL related events
  @VisibleForTesting
  private[atlas] val catalogEventTracker =
    new SparkCatalogEventProcessor(atlasClient, atlasClientConf)
  catalogEventTracker.startThread()

  // Processor to handle DML related events
  private val executionPlanTracker = new SparkExecutionPlanProcessor(atlasClient, atlasClientConf)
  executionPlanTracker.startThread()

  private val mlEventTracker = new MLPipelineEventProcessor(atlasClient, atlasClientConf)
  mlEventTracker.startThread()

  private val executionId = new AtomicLong(0L)

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    // We only care about SQL related events.
    event match {
      case e: ExternalCatalogEvent => catalogEventTracker.pushEvent(e)
      case e: SparkListenerEvent if e.getClass.getName.contains("org.apache.spark.ml") =>
        mlEventTracker.pushEvent(e)
      case _ => // Ignore other events
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    val qd = QueryDetail(qe, executionId.getAndIncrement(), durationNs, Option(SQLQuery.get()))
    executionPlanTracker.pushEvent(qd)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logWarn(s"Fail to execute query: {$qe}, {$funcName}", exception)
  }

  private def initializeSparkModel(): Boolean = {
    try {
      val checkModelInStart = atlasClientConf.get(AtlasClientConf.CHECK_MODEL_IN_START).toBoolean
      if (checkModelInStart) {
        val restClient = if (!atlasClient.isInstanceOf[RestAtlasClient]) {
          logWarn("Spark Atlas Model check and creation can only work with REST client, so " +
            "creating a new REST client")
          new RestAtlasClient(atlasClientConf)
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
