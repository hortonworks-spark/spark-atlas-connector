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
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.v1.model.notification.HookNotificationV1
import org.apache.atlas.v1.model.notification.HookNotificationV1.{EntityCreateRequest, EntityDeleteRequest}
import org.apache.atlas.v1.model.instance.Referenceable
import org.apache.atlas.model.notification.HookNotification
import com.hortonworks.spark.atlas.utils.SparkUtils

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
    val createRequests = entities.map { e =>
      new EntityCreateRequest(
        SparkUtils.currUser(), entityToReferenceable(e)): HookNotification
    }.toList.asJava

    notifyEntities(createRequests, SparkUtils.ugi())
  }

  override protected def doDeleteEntityWithUniqueAttr(
      entityType: String,
      attribute: String): Unit = {
    val deleteRequest = List(new EntityDeleteRequest(
      SparkUtils.currUser(),
      entityType,
      org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
      attribute): HookNotification)
    notifyEntities(deleteRequest.asJava, SparkUtils.ugi())
  }

  override protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    val partialUpdateRequest = List(
      new HookNotificationV1.EntityPartialUpdateRequest(
        SparkUtils.currUser(),
        entityType,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        attribute,
        entityToReferenceable(entity)): HookNotification)
    notifyEntities(partialUpdateRequest.asJava, SparkUtils.ugi())
  }

  private def entityToReferenceable(entity: AtlasEntity): Referenceable = {
    val classifications = Option(entity.getClassifications).map { s =>
      s.asScala.map(_.getTypeName)
    }.getOrElse(List.empty)

    val referenceable = new Referenceable(entity.getTypeName, classifications: _*)

    val convertedAttributes = entity.getAttributes.asScala.mapValues {
      case e: AtlasEntity =>
        entityToReferenceable(e)

      case l: util.Collection[_] =>
        val list = new util.ArrayList[Referenceable]()
        l.asScala.foreach {
          case e: AtlasEntity =>
            list.add(entityToReferenceable(e))
        }
        list

      case o => o
    }
    convertedAttributes.foreach { kv => referenceable.set(kv._1, kv._2) }

    referenceable
  }
}
