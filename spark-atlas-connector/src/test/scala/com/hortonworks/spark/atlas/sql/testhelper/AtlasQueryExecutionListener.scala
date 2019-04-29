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

import com.hortonworks.spark.atlas.AtlasUtils
import com.hortonworks.spark.atlas.sql.QueryDetail
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

class AtlasQueryExecutionListener extends QueryExecutionListener {
  val queryDetails = new mutable.MutableList[QueryDetail]()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (qe.logical.isStreaming) {
      // streaming query will be tracked via SparkAtlasStreamingQueryEventTracker
      return
    }
    queryDetails += QueryDetail(qe, AtlasUtils.issueExecutionId(), durationNs)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    throw exception
  }

  def clear(): Unit = {
    queryDetails.clear()
  }
}
