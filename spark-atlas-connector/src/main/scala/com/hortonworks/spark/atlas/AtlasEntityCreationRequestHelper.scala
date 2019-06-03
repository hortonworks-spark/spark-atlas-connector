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

import java.util.UUID

import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.utils.Logging
import org.apache.atlas.model.instance.AtlasObjectId

import scala.collection.mutable

class AtlasEntityCreationRequestHelper(
    atlasClient: AtlasClient) extends Logging {
  // query to (inputs, outputs)
  private val queryToInputsAndOutputs = new mutable.HashMap[UUID,
    (Set[AtlasObjectId], Set[AtlasObjectId])]()

  def requestCreation(entities: Seq[SACAtlasReferenceable], queryId: Option[UUID] = None): Unit = {
    queryId match {
      case Some(rid) => updateEntitiesForStreamingQuery(rid, entities)
      case None => updateEntitiesForBatchQuery(entities)
    }
  }

  private def updateEntitiesForBatchQuery(entities: Seq[SACAtlasReferenceable]): Unit = {
    // the query is batch, hence always create entities
    // create input/output entities as well as update process entity(-ies)
    createEntities(entities)
  }

  private def updateEntitiesForStreamingQuery(
      queryId: UUID,
      entities: Seq[SACAtlasReferenceable]): Unit = {
    // the query is streaming, so which partial of source/sink entities can be seen
    // in specific batch - need to accumulate efficiently
    val processes = entities
      .filter(en => en.typeName == metadata.PROCESS_TYPE_STRING
        && en.isInstanceOf[SACAtlasEntityWithDependencies])
      .map(_.asInstanceOf[SACAtlasEntityWithDependencies])

    val inputs = processes.flatMap { p =>
      AtlasEntityReadHelper.getSeqAtlasObjectIdAttribute(p.entity, "inputs")
    }.toSet

    val outputs = processes.flatMap { p =>
      AtlasEntityReadHelper.getSeqAtlasObjectIdAttribute(p.entity, "outputs")
    }.toSet

    queryToInputsAndOutputs.get(queryId) match {
      case Some((is, os)) if !inputs.subsetOf(is) || !outputs.subsetOf(os) =>
        // The query is streaming, and at least either inputs or outputs is not a
        // subset of accumulated one.

        // NOTE: we leverage the 'process' model's definition:
        // inputs and outputs are defined as set in definition, and Atlas automatically
        // accumulate these values which doesn't require us to track all inputs and
        // outputs and always provide accumulated one.
        // If we need to do in our own, we should also accumulate inputs and outputs
        // in SparkCatalogEventProcessor and maintain full of inputs and outputs.
        // Here we only accumulate inputs/outputs for each streaming query (queryId).

        createEntities(entities)

        // update inputs and outputs as accumulating current one and new inputs/outputs
        updateInputsAndOutputs(queryId, is.union(inputs), os.union(outputs))

      case Some((_, _)) => // if inputs.subsetOf(is) && outputs.subsetOf(os)
      // we already updated superset of inputs/outputs, skip updating

      case _ =>
        // the streaming query hasn't been examined in current session
        createEntities(entities)

        // update inputs and outputs as new inputs/outputs, as there's nothing to accumulate
        updateInputsAndOutputs(queryId, inputs, outputs)
    }
  }

  private def createEntities(entities: Seq[SACAtlasReferenceable]): Unit = {
    // create input/output entities as well as update process entity(-ies)
    atlasClient.createEntitiesWithDependencies(entities)
    logDebug(s"Created entities without columns")
  }

  private def updateInputsAndOutputs(
      queryId: UUID,
      newInputs: Set[AtlasObjectId],
      newOutputs: Set[AtlasObjectId]): Unit = {
    queryToInputsAndOutputs.put(queryId, (newInputs, newOutputs))
  }
}
