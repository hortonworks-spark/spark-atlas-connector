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

import scala.util.control.NonFatal

import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef

import com.hortonworks.spark.atlas.utils.Logging

trait AtlasClient extends Logging {

  def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit

  def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef

  def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit

  final def createEntity(entity: AtlasEntity): Unit = {
    try {
      doCreateEntity(entity)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to create entity: ${entity.toString}", e)
    }
  }

  protected def doCreateEntity(entity: AtlasEntity): Unit

  final def deleteEntityWithUniqueAttr(entityType: String, attribute: String): Unit = {
    try {
      doDeleteEntityWithUniqueAttr(entityType, attribute)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to delete entity with type $entityType", e)
    }
  }

  protected def doDeleteEntityWithUniqueAttr(entityType: String, attribute: String): Unit

  final def updateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit = {
    try {
      doUpdateEntityWithUniqueAttr(entityType, attribute, entity)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to update entity $entity with type $entityType and attribute " +
          s"$attribute", e)
    }
  }

  protected def doUpdateEntityWithUniqueAttr(
      entityType: String,
      attribute: String,
      entity: AtlasEntity): Unit
}

object AtlasClient {
  def atlasClient(conf: AtlasClientConf): AtlasClient = {
    conf.get(AtlasClientConf.CLIENT_TYPE).trim match {
      case "rest" => new RestAtlasClient(conf)
      case "kafka" => new KafkaAtlasClient(conf)
      case e =>
        Class.forName(e)
          .getConstructor(classOf[AtlasClientConf])
          .newInstance(conf)
          .asInstanceOf[AtlasClient]
    }
  }
}

