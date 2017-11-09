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

import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.typedef.AtlasTypesDef
import com.hortonworks.spark.atlas.utils.SparkUtils
import org.apache.atlas.model.instance.AtlasEntity

class KafkaAtlasClient(atlasClientConf: AtlasClientConf) extends AtlasHook with AtlasClient {

  private lazy val user = SparkUtils.sparkSession.sparkContext.sparkUser

  override protected def getNumberOfRetriesPropertyKey: String = {
    AtlasClientConf.CLIENT_NUM_RETRIES.key
  }

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support create type defs")
  }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support get type defs")
  }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support update type defs")
  }

  override protected def doCreateEntity(entity: AtlasEntity): Unit = { }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = { }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = { }
}
