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

  final def createEntitiesWithDependencies(
      entity: SACAtlasReferenceable): Unit = this.synchronized {
    entity match {
      case e: SACAtlasEntityWithDependencies =>
        // handle dependencies first
        if (e.dependencies.nonEmpty) {
          val deps = e.dependencies.filter(_.isInstanceOf[SACAtlasEntityWithDependencies])
            .map(_.asInstanceOf[SACAtlasEntityWithDependencies])

          val depsHavingAnotherDeps = deps.filter(_.dependencies.nonEmpty)
          val depsHavingNoDeps = deps.filterNot(_.dependencies.nonEmpty)

          // we should handle them one by one if they're having additional dependencies
          depsHavingAnotherDeps.foreach(createEntitiesWithDependencies)

          // otherwise, we can handle them at once
          createEntities(depsHavingNoDeps.map(_.entity))
        }

        // done with dependencies, process origin entity
        createEntities(Seq(e.entity))

      case _ => // don't request creation entity for reference
    }
  }

  final def createEntitiesWithDependencies(
      entities: Seq[SACAtlasReferenceable]): Unit = this.synchronized {
    entities.foreach(createEntitiesWithDependencies)
  }

  final def createEntities(entities: Seq[AtlasEntity]): Unit = this.synchronized {
    if (entities.isEmpty) {
      return
    }

    try {
      doCreateEntities(entities)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to create entities", e)
    }
  }

  protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit

  final def deleteEntityWithUniqueAttr(
      entityType: String, attribute: String): Unit = this.synchronized {
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
      entity: AtlasEntity): Unit = this.synchronized {
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
  @volatile private var client: AtlasClient = null

  def atlasClient(conf: AtlasClientConf): AtlasClient = {
    if (client == null) {
      AtlasClient.synchronized {
        if (client == null) {
          conf.get(AtlasClientConf.CLIENT_TYPE).trim match {
            case "rest" =>
              client = new RestAtlasClient(conf)
            case "kafka" =>
              client = new KafkaAtlasClient(conf)
            case e =>
              client = Class.forName(e)
                .getConstructor(classOf[AtlasClientConf])
                .newInstance(conf)
                .asInstanceOf[AtlasClient]
          }
        }
      }
    }

    client
  }
}

