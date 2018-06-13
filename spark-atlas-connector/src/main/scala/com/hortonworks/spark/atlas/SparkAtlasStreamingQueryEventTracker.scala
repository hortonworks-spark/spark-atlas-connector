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

import scala.collection.mutable
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import com.hortonworks.spark.atlas.sql.SparkStreamingQueryEventProcessor
import com.hortonworks.spark.atlas.utils.Logging

class SparkAtlasStreamingQueryEventTracker(
     atlasClient: AtlasClient,
     atlasClientConf: AtlasClientConf)
  extends StreamingQueryListener with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val streamQueryHashset = new mutable.HashSet[java.util.UUID]

  private val streamingQueryTracker =
    new SparkStreamingQueryEventProcessor(atlasClient, atlasClientConf)
  streamingQueryTracker.startThread()

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    logDebug(s"Start to track the Spark Streaming query in the Spark Atlas $event")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    logInfo(s"Track running Spark Streaming query in the Spark Atlas: $event")
    if(!streamQueryHashset.contains(event.progress.runId)) {
      streamingQueryTracker.pushEvent(event)
      streamQueryHashset.add(event.progress.runId)
    }
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    streamQueryHashset.remove(event.runId)
    logDebug(s"Tack Spark Streaming query in the Spark Atlas Terminated: $event")
  }
}
