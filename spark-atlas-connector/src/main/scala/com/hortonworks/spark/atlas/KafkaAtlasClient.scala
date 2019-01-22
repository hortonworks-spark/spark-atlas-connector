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

import java.util

import scala.collection.JavaConverters._
import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.atlas.v1.model.notification.HookNotificationV1
import org.apache.atlas.v1.model.notification.HookNotificationV1.{EntityCreateRequest, EntityDeleteRequest}
import org.apache.atlas.v1.model.instance.Referenceable
import org.apache.atlas.model.notification.HookNotification
import com.hortonworks.spark.atlas.utils.SparkUtils
import org.apache.atlas.AtlasClientV2.API_V2
import org.apache.atlas.model.instance.AtlasEntity.{AtlasEntitiesWithExtInfo, AtlasEntityWithExtInfo}
import org.apache.atlas.model.notification.HookNotification.{EntityCreateRequestV2, EntityDeleteRequestV2, EntityPartialUpdateRequestV2}

class KafkaAtlasClient(atlasClientConf: AtlasClientConf) extends AtlasHook with AtlasClient {

   protected def getNumberOfRetriesPropertyKey: String = {
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

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    val entitiesWithExtInfo = new AtlasEntitiesWithExtInfo()
    entities.foreach(entitiesWithExtInfo.addEntity)
    val createRequest = new EntityCreateRequestV2(
      SparkUtils.currUser(), entitiesWithExtInfo): HookNotification

    notifyEntities(Seq(createRequest).asJava, SparkUtils.ugi())
  }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = {
    val deleteRequest = new EntityDeleteRequestV2(
      SparkUtils.currUser(),
      Seq(new AtlasObjectId(entityType,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        attribute)).asJava
    ): HookNotification

    notifyEntities(Seq(deleteRequest).asJava, SparkUtils.ugi())
  }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    val partialUpdateRequest = new EntityPartialUpdateRequestV2(
      SparkUtils.currUser(),
      new AtlasObjectId(entityType,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        attribute),
      new AtlasEntityWithExtInfo(entity)
    ): HookNotification

    notifyEntities(Seq(partialUpdateRequest).asJava, SparkUtils.ugi())
  }
}
