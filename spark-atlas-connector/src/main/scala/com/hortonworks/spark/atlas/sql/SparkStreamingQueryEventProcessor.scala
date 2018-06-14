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

import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.Logging
import org.apache.atlas.model.instance.AtlasEntity


class SparkStreamingQueryEventProcessor (
      private[atlas] val atlasClient: AtlasClient,
      val conf: AtlasClientConf)
extends AbstractEventProcessor[QueryProgressEvent] with AtlasEntityUtils with Logging {

  override def process(e: QueryProgressEvent): Unit = {

    val inputEntities = e.progress.sources.map{
      case s if (s.description.contains("FileStreamSource")) =>
        val begin = s.description.indexOf('[')
        val end = s.description.indexOf(']')
        val path = s.description.substring(begin + 1, end)
        logDebug(s"record the streaming query sink input path information $path")
        external.pathToEntity(path)
    }

    var outputEntity: AtlasEntity = null
    if (e.progress.sink.description.contains("FileSink")) {
      val begin = e.progress.sink.description.indexOf('[')
      val end = e.progress.sink.description.indexOf(']')
      val path = e.progress.sink.description.substring(begin + 1, end)
      logDebug(s"record the streaming query sink output path information $path")
      outputEntity = external.pathToEntity(path)
    } else if (e.progress.sink.description.contains("ConsoleSinkProvider")) {
      logInfo(s"do not track the console output as Atlas entity ${e.progress.sink.description}")
      return
    }

    val pName = e.progress.name match {
      case name: String => name
      case _ => "unknown"
    }

    val logMap = Map("executionId" -> e.progress.batchId.toString,
      "details" -> e.progress.json,
      "sparkPlanDescription" -> s"Spark StreamingQueryPorgress ${pName}")

    val entities = {
      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputEntities, Seq(outputEntity), logMap)
      } else {
        val pEntity = internal.etlProcessToEntity(
          inputEntities.toList, List(outputEntity), logMap)

        Seq(pEntity) ++ inputEntities ++ Seq(outputEntity)
      }
    }
    atlasClient.createEntities(entities)
    logInfo(s"create the altas entity for Spark Streaming Query Processing Event ${e.progress.id}")
  }
}
