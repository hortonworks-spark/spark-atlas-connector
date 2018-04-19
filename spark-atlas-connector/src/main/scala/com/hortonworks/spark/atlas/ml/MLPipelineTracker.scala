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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.annotations.VisibleForTesting
import com.hortonworks.spark.atlas.AbstractService
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, SparkAtlasModel, external, internal}
import com.hortonworks.spark.atlas.utils.{CatalogUtils, Logging, SparkUtils}
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, RestAtlasClient}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.ml._
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable
import scala.util.control.NonFatal

class MLPipelineTracker(
    val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends SparkListener with AbstractService with AtlasEntityUtils with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  // A blocking queue for Spark Listener ML related events.
  @VisibleForTesting
  private[atlas] val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)

  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  @VisibleForTesting
  @volatile private[atlas] var shouldContinue: Boolean = true

  // private val cachedObjects = new mutable.HashMap[String, Object]

  private val uri = "hdfs://"

  startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    // We only care about ML related events.
    event match {
      case e: SparkListenerEvent if e.getClass.getName.contains("org.apache.spark.ml") =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }

      case _ =>
      // Ignore other events
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

  // Skeleton to track ML pipeline
  @VisibleForTesting
  protected[atlas] override def eventProcess(): Unit = {
    // initialize Atlas client before further processing event.
    if (!initializeSparkModel()) {
      logError("Fail to initialize Atlas Client, will discard all the received events and stop " +
        "working")

      shouldContinue = false
      eventQueue.clear()
      return
    }

    var stopped = false

    while (!stopped) {
      try {
        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach {
          event => event.getClass.getName match {

            case name if name.contains("CreatePipelineEvent") =>

              val datasetF = event.getClass.getDeclaredField("dataset")
              datasetF.setAccessible(true)
              val dataset = datasetF.get(event).asInstanceOf[Dataset[_]]

              val pipelineF = event.getClass.getDeclaredField("pipeline")
              pipelineF.setAccessible(true)
              val pipeline = pipelineF.get(event).asInstanceOf[Pipeline]

              internal.cachedObjects.put(pipeline.uid, pipeline)
              internal.cachedObjects.put(pipeline.uid + "_" + "traindata", dataset)

              val logicalplan = dataset.queryExecution.analyzed

              val entities = logicalplan.collectLeaves().map {
                case l: LogicalRelation => l.relation match {
                  case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
                  case _ => Seq.empty
                }
              }


            case name if name.contains("CreateModelEvent") =>
              val modeF = event.getClass.getDeclaredField("model")
              modeF.setAccessible(true)
              val model = modeF.get(event).asInstanceOf[PipelineModel]
              internal.cachedObjects.put(model.uid + "_" + "model", model)

            case name if name.contains("SavePipelineEvent") =>
              val uidF = event.getClass.getDeclaredField("uid")
              uidF.setAccessible(true)
              val uid = uidF.get(event).asInstanceOf[String]

              val pathF = event.getClass.getDeclaredField("directory")
              pathF.setAccessible(true)
              val path = pathF.get(event).asInstanceOf[String]

              val pipelineDirEntity = internal.mlDirectoryToEntity(uri, path)
              val pipeline = internal.cachedObjects.get(uid).get.asInstanceOf[Pipeline]

              val pipelineEntity = internal.mlPipelineToEntity(pipeline, pipelineDirEntity)
              atlasClient.createEntities(Seq(pipelineEntity, pipelineDirEntity))

              internal.cachedObjects.put(uid + "_" + "pipelineDirEntity", pipelineDirEntity)
              internal.cachedObjects.put(uid + "_" + "pipelineEntity", pipelineEntity)

              logInfo(s"Created pipeline Entity " + pipelineEntity.getGuid)

            case name if name.contains("SaveModelEvent") =>

              val uidF = event.getClass.getDeclaredField("uid")
              uidF.setAccessible(true)
              val uid = uidF.get(event).asInstanceOf[String]

              val pathF = event.getClass.getDeclaredField("directory")
              pathF.setAccessible(true)
              val path = pathF.get(event).asInstanceOf[String]

              if (!internal.cachedObjects.contains(uid + "_" + "pipelineDirEntity")) {

                logInfo(s"Model Entity is already created")
              } else {

                val modelDirEntity = internal.mlDirectoryToEntity(uri, path)

                val pipelineDirEntity = internal.cachedObjects.get(uid + "_" + "pipelineDirEntity")
                  .get.asInstanceOf[AtlasEntity]
                val pipelineEntity = internal.cachedObjects.get(uid + "_" + "pipelineEntity")
                  .get.asInstanceOf[AtlasEntity]
                val pipeline = internal.cachedObjects.get(uid).get.asInstanceOf[Pipeline]

                atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity))
                val model = internal.cachedObjects.get(uid + "_" + "model").
                  get.asInstanceOf[PipelineModel]

                val modelEntity = internal.mlModelToEntity(model, modelDirEntity)

                atlasClient.createEntities(Seq(modelEntity, modelDirEntity))

                val trainingdata = internal.cachedObjects.get(pipeline.uid + "_" + "traindata")
                  .get.asInstanceOf[Dataset[_]]

                val logicalplan = trainingdata.queryExecution.analyzed

                var isFiles = false
                val tableEntities = logicalplan.collectLeaves().map {
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
                    logWarn(s"Missing unknown leaf node: $e")
                    Seq.empty
                }

                val fitEntity = internal.mlFitProcessToEntity(
                  pipeline,
                  pipelineEntity,
                  List(pipelineEntity, tableEntities.head.head),
                  List(modelEntity))

                val logMap = Map("currUser" -> SparkUtils.currUser())

                val processEntity = internal.mlProcessToEntity(
                  List(pipelineEntity, tableEntities.head.head), List(modelEntity), logMap)

                atlasClient.createEntities(Seq(pipelineDirEntity, pipelineEntity, processEntity)
                  ++ Seq(modelDirEntity, modelEntity) ++ tableEntities.head)

                internal.cachedObjects.remove(uid + "_" + "pipelineDirEntity")
                internal.cachedObjects.remove(uid + "_" + "pipelineEntity")
                internal.cachedObjects.remove(uid + "_" + "model")
                internal.cachedObjects.remove(uid + "_" + "traindata")
                internal.cachedObjects.remove(uid)

                internal.cachedObjects.put("fit_process", uid)

                logInfo(s"Created pipeline fitEntity " + fitEntity.getGuid)
              }


            case name if name.contains("LoadModelEvent") =>

              val modeF = event.getClass.getDeclaredField("model")
              modeF.setAccessible(true)
              val model = modeF.get(event).asInstanceOf[PipelineModel]

              val directoryF = event.getClass.getDeclaredField("directory")
              directoryF.setAccessible(true)
              val directory = directoryF.get(event).asInstanceOf[String]

              val modelDirEntity = internal.mlDirectoryToEntity(uri, directory)
              val modelEntity = internal.mlModelToEntity(model, modelDirEntity)

              atlasClient.createEntities(Seq(modelEntity, modelDirEntity))

              val uid = model.uid
              internal.cachedObjects.put(uid + "_" + "modelDirEntity", modelDirEntity)
              internal.cachedObjects.put(uid + "_" + "modelEntity", modelEntity)

              logInfo(s"Created model Entity " + modelEntity.getGuid)

            case name if name.contains("TransformEvent") =>

              val modeF = event.getClass.getDeclaredField("model")
              modeF.setAccessible(true)
              val model = modeF.get(event).asInstanceOf[PipelineModel]

              val uid = model.uid

              internal.cachedObjects.put("model_uid", uid)

              logInfo(s"Cache for TransformEvent " + uid)
            case _ =>
              logInfo(s"ML tracker does not support for other events")

          }
        }
      }
    }



  }

  private def initializeSparkModel(): Boolean = {
    try {
      val checkModelInStart = conf.get(AtlasClientConf.CHECK_MODEL_IN_START).toBoolean
      if (checkModelInStart) {
        val restClient = if (!atlasClient.isInstanceOf[RestAtlasClient]) {
          logWarn("Spark Atlas Model check and creation can only work with REST client, so " +
            "creating a new REST client")
          new RestAtlasClient(conf)
        } else {
          atlasClient
        }

        SparkAtlasModel.checkAndCreateTypes(restClient)
      }

      true
    } catch {
      case NonFatal(e) =>
        logError(s"Fail to initialize Atlas client, stop this listener", e)
        false
    }
  }
}
