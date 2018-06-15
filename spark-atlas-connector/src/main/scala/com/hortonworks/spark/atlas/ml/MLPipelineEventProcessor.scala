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

package com.hortonworks.spark.atlas.ml

import org.apache.atlas.model.instance.AtlasEntity

import org.apache.spark.ml._
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation

import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.Logging

class MLPipelineEventProcessor(
  private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[SparkListenerEvent] with AtlasEntityUtils with Logging {

  override def process(e: SparkListenerEvent): Unit = {
    e.getClass.getName match {
      case name if name.contains("CreatePipelineEvent") =>
        val datasetF = e.getClass.getDeclaredField("dataset")
        datasetF.setAccessible(true)
        val dataset = datasetF.get(e).asInstanceOf[Dataset[_]]

        val pipelineF = e.getClass.getDeclaredField("pipeline")
        pipelineF.setAccessible(true)
        val pipeline = pipelineF.get(e).asInstanceOf[Pipeline]

        internal.cachedObjects.put(pipeline.uid, pipeline)
        internal.cachedObjects.put(s"${pipeline.uid}_traindata", dataset)

      case name if name.contains("CreateModelEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]
        internal.cachedObjects.put(s"${model.uid}_model", model)

      case name if name.contains("SavePipelineEvent") =>
        val uidF = e.getClass.getDeclaredField("uid")
        uidF.setAccessible(true)
        val uid = uidF.get(e).asInstanceOf[String]

        val pathF = e.getClass.getDeclaredField("directory")
        pathF.setAccessible(true)
        val path = pathF.get(e).asInstanceOf[String]

        val pipelineDirEntity = external.pathToEntity(path)
        val pipeline = internal.cachedObjects(uid).asInstanceOf[Pipeline]

        val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
        atlasClient.createEntities(Seq(pipelineEntity, pipelineDirEntity))

        internal.cachedObjects.put(s"${uid}_pipelineDirEntity", pipelineDirEntity)
        internal.cachedObjects.put(s"${uid}_pipelineEntity", pipelineEntity)

        logInfo(s"Created pipeline Entity ${pipelineEntity.getGuid}")

      case name if name.contains("SaveModelEvent") =>

        val uidF = e.getClass.getDeclaredField("uid")
        uidF.setAccessible(true)
        val uid = uidF.get(e).asInstanceOf[String]

        val pathF = e.getClass.getDeclaredField("directory")
        pathF.setAccessible(true)
        val path = pathF.get(e).asInstanceOf[String]

        if (! internal.cachedObjects.contains(s"${uid}_pipelineDirEntity")) {
          logInfo(s"Model Entity is already created")
        } else {
          val modelDirEntity = external.pathToEntity(path)

          val pipelineDirEntity = internal.cachedObjects(s"${uid}_pipelineDirEntity")
            .asInstanceOf[AtlasEntity]
          val pipelineEntity = internal.cachedObjects(s"${uid}_pipelineEntity")
            .asInstanceOf[AtlasEntity]
          val pipeline = internal.cachedObjects.get(uid).get.asInstanceOf[Pipeline]
          atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity))

          val model = internal.cachedObjects(s"${uid}_model").asInstanceOf[PipelineModel]
          val modelEntity = internal.mlModelToEntity(model, modelDirEntity)
          atlasClient.createEntities(Seq(modelEntity, modelDirEntity))

          val trainData = internal.cachedObjects(s"${pipeline.uid}_traindata")
            .asInstanceOf[Dataset[_]]

          val logicalPlan = trainData.queryExecution.analyzed
          var isFiles = false
          val tableEntities = logicalPlan.collectLeaves().map {
            case r: HiveTableRelation => tableToEntities(r.tableMeta)
            case v: View => tableToEntities(v.desc)
            case l: LogicalRelation if l.catalogTable.isDefined =>
              l.catalogTable.map(tableToEntities(_)).get
            case l: LogicalRelation =>
              isFiles = true
              l.relation match {
                case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
                case _ => Seq.empty
              }
            case e =>
              logWarn(s"Missing unknown leaf node for Sparm ML model training input: $e")
              Seq.empty
          }

          val logMap = Map("sparkPlanDescription" ->
            (s"Spark ML training model with pipeline uid: ${pipeline.uid}"))

          val processEntity = internal.etlProcessToEntity(
            List(pipelineEntity, tableEntities.head.head),
            List(modelEntity, modelDirEntity, pipelineDirEntity), logMap)

          atlasClient.createEntities(Seq(pipelineDirEntity, pipelineEntity, processEntity)
            ++ Seq(modelDirEntity, modelEntity) ++ tableEntities.head)

          internal.cachedObjects.put("fit_process", processEntity.getGuid)
          logInfo(s"Created pipeline fitEntity: ${processEntity.getGuid}")
        }

      case name if name.contains("LoadModelEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]

        val directoryF = e.getClass.getDeclaredField("directory")
        directoryF.setAccessible(true)
        val directory = directoryF.get(e).asInstanceOf[String]

        val modelDirEntity = external.pathToEntity(directory)
        val modelEntity = internal.mlModelToEntity(model, modelDirEntity)
        val uid = model.uid
        internal.cachedObjects.put(s"${uid}_modelDirEntity", modelDirEntity)
        internal.cachedObjects.put(s"${uid}_modelEntity", modelEntity)

      case name if name.contains("TransformEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]
        val uid = model.uid
        internal.cachedObjects.put("model_uid", uid)
        logInfo(s"Cache for TransformEvent $uid")
      case _ =>
        logInfo(s"ML tracker does not support for other events")
    }
  }

}
