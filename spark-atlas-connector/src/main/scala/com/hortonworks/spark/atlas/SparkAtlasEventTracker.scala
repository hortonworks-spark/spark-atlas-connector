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

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import com.hortonworks.spark.atlas.sql._
import com.hortonworks.spark.atlas.ml.MLPipelineEventProcessor
import com.hortonworks.spark.atlas.utils.Logging

class SparkAtlasEventTracker(atlasClient: AtlasClient, atlasClientConf: AtlasClientConf)
    extends SparkListener with QueryExecutionListener with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val enabled: Boolean = AtlasUtils.isSacEnabled(atlasClientConf)

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

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!enabled) {
      // No op if SAC is disabled
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
    if (!enabled) {
      // No op if SAC is disabled
      return
    }

    if (qe.logical.isStreaming) {
      // streaming query will be tracked via SparkAtlasStreamingQueryEventTracker
      return
    }

    val qd = QueryDetail.fromQueryExecutionListener(qe, durationNs)
    executionPlanTracker.pushEvent(qd)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    // No-op: SAC is one of the listener.
  }

}
