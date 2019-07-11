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

import com.hortonworks.spark.atlas.utils.Logging
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

object AtlasUtils extends Logging {
  private val executionId = new AtomicLong(0L)

  def entityToReference(entity: AtlasEntity, useGuid: Boolean = false): AtlasObjectId = {
    if (useGuid) {
      new AtlasObjectId(entity.getGuid)
    } else {
      new AtlasObjectId(entity.getTypeName, "qualifiedName", entity.getAttribute("qualifiedName"))
    }
  }

  def entitiesToReferences(
      entities: Seq[AtlasEntity],
      useGuid: Boolean = false): Set[AtlasObjectId] = {
    entities.map(entityToReference(_, useGuid)).toSet
  }

  def issueExecutionId(): Long = executionId.getAndIncrement()

  def isSacEnabled(conf: AtlasClientConf): Boolean = {
    if (!conf.get(AtlasClientConf.ATLAS_SPARK_ENABLED).toBoolean) {
      logWarn("Spark Atlas Connector is disabled.")
      false
    } else {
      true
    }
  }
}
