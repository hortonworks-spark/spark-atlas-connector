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

import org.apache.atlas.model.instance.AtlasEntity
import scala.collection.convert.Wrappers.SeqWrapper

object AtlasEntityReadHelper {
  def listAtlasEntitiesAsType(entities: Seq[AtlasEntity], typeStr: String): Seq[AtlasEntity] = {
    entities.filter(p => p.getTypeName.equals(typeStr))
  }

  def getOnlyOneEntity(entities: Seq[AtlasEntity], typeStr: String): AtlasEntity = {
    val filteredEntities = entities.filter { p =>
      p.getTypeName.equals(typeStr)
    }
    assert(filteredEntities.size == 1)
    filteredEntities.head
  }

  def getStringAttribute(entity: AtlasEntity, attrName: String): String = {
    entity.getAttribute(attrName).asInstanceOf[String]
  }

  def getAtlasEntityAttribute(entity: AtlasEntity, attrName: String): AtlasEntity = {
    entity.getAttribute(attrName).asInstanceOf[AtlasEntity]
  }

  def getSeqAtlasEntityAttribute(entity: AtlasEntity, attrName: String)
  : Seq[AtlasEntity] = {
    entity.getAttribute(attrName).asInstanceOf[SeqWrapper[AtlasEntity]].underlying
  }
}
