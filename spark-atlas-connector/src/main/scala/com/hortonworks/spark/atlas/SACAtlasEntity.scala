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

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

trait SACAtlasReferenceable {
  def typeName: String
  def qualifiedName: String
  def asObjectId: AtlasObjectId
}

case class SACAtlasEntityReference(ref: AtlasObjectId) extends SACAtlasReferenceable {
  require(typeName != null && !typeName.isEmpty)
  require(qualifiedName != null && !qualifiedName.isEmpty)

  override def typeName: String = ref.getTypeName

  override def qualifiedName: String = ref.getUniqueAttributes.get(
    org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString

  override def asObjectId: AtlasObjectId = ref
}

case class SACAtlasEntityWithDependencies(
    entity: AtlasEntity,
    dependencies: Seq[SACAtlasReferenceable]) extends SACAtlasReferenceable {

  require(typeName != null && !typeName.isEmpty)
  require(qualifiedName != null && !qualifiedName.isEmpty)

  override def typeName: String = entity.getTypeName

  override def qualifiedName: String = entity.getAttribute(
    org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString

  override def asObjectId: AtlasObjectId = AtlasUtils.entityToReference(entity, useGuid = false)

  def dependenciesAdded(deps: Seq[SACAtlasReferenceable]): SACAtlasEntityWithDependencies = {
    new SACAtlasEntityWithDependencies(entity, dependencies ++ deps)
  }
}

object SACAtlasEntityWithDependencies {
  def apply(entity: AtlasEntity): SACAtlasEntityWithDependencies = {
    new SACAtlasEntityWithDependencies(entity, Seq.empty)
  }
}
