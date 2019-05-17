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

package com.hortonworks.spark.atlas.sql.testhelper

import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.utils.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}

import scala.collection.mutable
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class AtlasStreamingQueryProgressListener extends StreamingQueryListener with Logging {
  val queryDetails = new mutable.MutableList[QueryDetail]()

  def onQueryStarted(event: QueryStartedEvent): Unit = {}

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    // FIXME: this is totally duplicated with SparkAtlasStreamingQueryEventTracker...
    //  Extract into somewhere...
    val query = SparkSession.active.streams.get(event.progress.id)
    if (query != null) {
      query match {
        case query: StreamingQueryWrapper =>
          val qd = QueryDetail.fromStreamingQueryListener(query.streamingQuery, event)
          queryDetails += qd

        case query: StreamExecution =>
          val qd = QueryDetail.fromStreamingQueryListener(query, event)
          queryDetails += qd

        case _ => logWarn(s"Unexpected type of streaming query: ${query.getClass}")
      }
    } else {
      logWarn(s"Cannot find query ${event.progress.id} from active spark session!")
    }
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}

  def clear(): Unit = {
    queryDetails.clear()
  }
}
