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

import java.util

import com.hortonworks.spark.atlas.AtlasEntityReadHelper.getOnlyOneEntity
import com.hortonworks.spark.atlas.types.metadata

import scala.collection.JavaConverters._
import com.hortonworks.spark.atlas.{SACAtlasEntityWithDependencies, SACAtlasReferenceable, AtlasUtils, TestUtils}
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.scalatest.FunSuite

trait ProcessEntityValidator extends FunSuite {
  def validateProcessEntity(
                             process: SACAtlasReferenceable,
                             validateFnForProcess: AtlasEntity => Unit,
                             validateFnForInputs: Seq[SACAtlasReferenceable] => Unit,
                             validateFnForOutputs: Seq[SACAtlasReferenceable] => Unit): Unit = {
    require(process.isInstanceOf[SACAtlasEntityWithDependencies])
    val pEntity = process.asInstanceOf[SACAtlasEntityWithDependencies].entity
    validateFnForProcess(pEntity)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasObjectId]]
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasObjectId]]

    val pDeps = process.asInstanceOf[SACAtlasEntityWithDependencies].dependencies
    val inputEntities = TestUtils.findEntities(pDeps, inputs.asScala.toSeq)
    val outputEntities = TestUtils.findEntities(pDeps, outputs.asScala.toSeq)

    assert(inputs.size() === inputEntities.size)
    assert(outputs.size() === outputEntities.size)

    validateFnForInputs(inputEntities)
    validateFnForOutputs(outputEntities)
  }

  def validateProcessEntityWithAtlasEntities(
      entities: Seq[AtlasEntity],
      validateFnForProcess: AtlasEntity => Unit,
      expectedInputObjectIds: Set[AtlasObjectId],
      expectedOutputObjectIds: Set[AtlasObjectId]): Unit = {
    val pEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)
    validateFnForProcess(pEntity)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasObjectId]]
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasObjectId]]

    assert(inputs.asScala.toSet === expectedInputObjectIds)
    assert(outputs.asScala.toSet === expectedOutputObjectIds)
  }

  def validateProcessEntityWithAtlasEntitiesForStreamingQuery(
      entities: Seq[AtlasEntity],
      validateFnForProcess: AtlasEntity => Unit,
      expectedInputEntities: Seq[AtlasEntity],
      expectedOutputEntities: Seq[AtlasEntity]): Unit = {
    val pEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)
    validateFnForProcess(pEntity)

    assert(pEntity.getAttribute("inputs").isInstanceOf[util.Collection[_]])
    assert(pEntity.getAttribute("outputs").isInstanceOf[util.Collection[_]])
    val inputs = pEntity.getAttribute("inputs").asInstanceOf[util.Collection[AtlasObjectId]]
    val outputs = pEntity.getAttribute("outputs").asInstanceOf[util.Collection[AtlasObjectId]]

    val expectedInputObjectIds = expectedInputEntities.map { entity =>
      AtlasUtils.entityToReference(entity, useGuid = false)
    }.toSet
    val expectedOutputObjectIds = expectedOutputEntities.map { entity =>
      AtlasUtils.entityToReference(entity, useGuid = false)
    }.toSet

    assert(inputs.asScala.toSet === expectedInputObjectIds)
    assert(outputs.asScala.toSet === expectedOutputObjectIds)
  }
}
