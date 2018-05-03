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

import scala.collection.mutable

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.ml._
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.Logging
import com.hortonworks.spark.atlas.utils.CatalogUtils

class MLPipelineEventProcessor(
  private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[SparkListenerEvent] with AtlasEntityUtils with Logging {

  private val cachedObjects = new mutable.HashMap[String, Object]

  private val uri = "hdfs://"

  override def process(e: SparkListenerEvent): Unit = {
    e.getClass.getName match {
      case name if name.contains("CreatePipelineEvent") =>
        val datasetF = e.getClass.getDeclaredField("dataset")
        datasetF.setAccessible(true)
        val dataset = datasetF.get(e).asInstanceOf[Dataset[_]]

        val pipelineF = e.getClass.getDeclaredField("pipeline")
        pipelineF.setAccessible(true)
        val pipeline = pipelineF.get(e).asInstanceOf[Pipeline]

        cachedObjects.put(pipeline.uid, pipeline)
        cachedObjects.put(s"${pipeline.uid}_traindata", dataset)

        val logicalPlan = dataset.queryExecution.analyzed

        val entities = logicalPlan.collectLeaves().map {
          case l: LogicalRelation => l.relation match {
            case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
            case _ => Seq.empty
          }
        }

      case name if name.contains("CreateModelEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]
        cachedObjects.put(s"${model.uid}_model", model)

      case name if name.contains("SavePipelineEvent") =>
        val uidF = e.getClass.getDeclaredField("uid")
        uidF.setAccessible(true)
        val uid = uidF.get(e).asInstanceOf[String]

        val pathF = e.getClass.getDeclaredField("directory")
        pathF.setAccessible(true)
        val path = pathF.get(e).asInstanceOf[String]

        val pipelineDirEntity = internal.mlDirectoryToEntity(uri, path)
        val pipeline = cachedObjects(uid).asInstanceOf[Pipeline]

        val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
        atlasClient.createEntities(Seq(pipelineEntity, pipelineDirEntity))

        cachedObjects.put(s"${uid}_pipelineDirEntity", pipelineDirEntity)
        cachedObjects.put(s"${uid}_pipelineEntity", pipelineEntity)

        logInfo(s"Created pipeline Entity ${pipelineEntity.getGuid}")

      case name if name.contains("SaveModelEvent") =>

        val uidF = e.getClass.getDeclaredField("uid")
        uidF.setAccessible(true)
        val uid = uidF.get(e).asInstanceOf[String]

        val pathF = e.getClass.getDeclaredField("directory")
        pathF.setAccessible(true)
        val path = pathF.get(e).asInstanceOf[String]

        if (!cachedObjects.contains(s"${uid}_pipelineDirEntity")) {
          logInfo(s"Model Entity is already created")
        } else {
          val modelDirEntity = internal.mlDirectoryToEntity(uri, path)

          val pipelineDirEntity = cachedObjects(s"${uid}_pipelineDirEntity")
            .asInstanceOf[AtlasEntity]
          val pipelineEntity = cachedObjects(s"${uid}_pipelineEntity").asInstanceOf[AtlasEntity]
          val pipeline = cachedObjects.get(uid).get.asInstanceOf[Pipeline]
          atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity))

          val model = cachedObjects(s"${uid}_model").asInstanceOf[PipelineModel]
          val modelEntity = internal.mlModelToEntity(model, modelDirEntity)
          atlasClient.createEntities(Seq(modelEntity, modelDirEntity))

          val trainingdata = cachedObjects(s"${pipeline.uid}_traindata").asInstanceOf[Dataset[_]]
          val logicalPlan = trainingdata.queryExecution.analyzed
          val tableEntities = logicalPlan.collectLeaves().map {
            case l: LogicalRelation => l.relation match {
              case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
              case _ => Seq.empty
            }
          }

          val fitEntity = internal.mlFitProcessToEntity(
            pipeline,
            pipelineEntity,
            List(pipelineEntity, tableEntities.head.head),
            List(modelEntity))

          atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity,
            pipelineEntity, modelEntity, fitEntity) ++ tableEntities.head)

          cachedObjects.remove(s"${uid}_pipelineDirEntity")
          cachedObjects.remove(s"${uid}_pipelineEntity")
          cachedObjects.remove(s"${uid}_model")
          cachedObjects.remove(s"${uid}_traindata")
          cachedObjects.remove(uid)

          logInfo(s"Created pipeline fitEntity " + fitEntity.getGuid)
        }

      case name if name.contains("LoadModelEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]

        val directoryF = e.getClass.getDeclaredField("directory")
        directoryF.setAccessible(true)
        val directory = directoryF.get(e).asInstanceOf[String]

        val modelDirEntity = internal.mlDirectoryToEntity(uri, directory)
        val modelEntity = internal.mlModelToEntity(model, modelDirEntity)
        val uid = model.uid
        cachedObjects.put(s"${uid}_modelDirEntity", modelDirEntity)
        cachedObjects.put(s"${uid}_modelEntity", modelEntity)

      case name if name.contains("TransformEvent") =>
        val modeF = e.getClass.getDeclaredField("model")
        modeF.setAccessible(true)
        val model = modeF.get(e).asInstanceOf[PipelineModel]

        val datasetInputF = e.getClass.getDeclaredField("input")
        datasetInputF.setAccessible(true)
        val inputdataset = datasetInputF.get(e).asInstanceOf[Dataset[_]]

        val datasetOutputF = e.getClass.getDeclaredField("output")
        datasetOutputF.setAccessible(true)
        val outputdataset = datasetOutputF.get(e).asInstanceOf[Dataset[_]]
        val uid = model.uid

        if (cachedObjects.contains(s"${uid}_modelEntity")) {
          val logicalplan = inputdataset.queryExecution.analyzed
          val tableEntities2 = logicalplan.collectLeaves().map {
            case l: LogicalRelation => l.relation match {
              case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
              case _ => Seq.empty
            }
          }

          val name = outputdataset.hashCode().toString
          val tableEntities3 = getTableEntities(name)

          val modelEntity = cachedObjects(s"${uid}_modelEntity").asInstanceOf[AtlasEntity]
          val modelDirEntity = cachedObjects(s"${uid}_modelDirEntity").asInstanceOf[AtlasEntity]

          val transformEntity = internal.mlTransformProcessToEntity(
            model,
            modelEntity,
            List(modelEntity, tableEntities2.head.head),
            List(tableEntities3.head))

          atlasClient.createEntities(Seq(modelDirEntity, modelEntity, transformEntity)
            ++ tableEntities2.head ++ tableEntities3)

          cachedObjects.remove(s"${uid}_modelEntity")
          cachedObjects.remove(s"${uid}_modelDirEntity")
          logInfo(s"Created transFormEntity ${transformEntity.getGuid}")
        } else {
          logInfo(s"Transform Entity is already created")
        }

      case _ =>
        logInfo(s"ML tracker does not support for other events")
    }
  }

  // Return table related entities as a Sequence.
  // The first one is table entity, followed by
  // db entity, storage entity and schema entities.
  def getTableEntities(tableName: String): Seq[AtlasEntity] = {
    val dbDefinition = CatalogUtils.createDB("db1", "hdfs:///test/db/db1")
    val sd = CatalogUtils.createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = CatalogUtils.createTable("db1", s"$tableName", schema, sd)
    val tableEntities = internal.sparkTableToEntities(tableDefinition, Some(dbDefinition))

    tableEntities
  }
}
