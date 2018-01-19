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
import com.hortonworks.spark.atlas.sql.AbstractService
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, SparkAtlasModel, internal}
import com.hortonworks.spark.atlas.utils.Logging
import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, RestAtlasClient}
import com.hortonworks.spark.atlas.utils.CatalogUtils

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.ml._
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable
import scala.util.control.NonFatal

class MLPipelineTracker(
    val atlasClient: AtlasClient,
    val conf: AtlasClientConf)  extends SparkListener with AbstractService with AtlasEntityUtils with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  // A blocking queue for Spark Listener ExternalCatalog related events.
  @VisibleForTesting
  private[atlas] val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)

  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  @VisibleForTesting
  @volatile private[atlas] var shouldContinue: Boolean = true

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  private var pipelineCache:Pipeline = null

  private var mlModeCache:PipelineModel = null

  private var pipelineDirectoryCache:String = null

  private var modelDirectoryCache:String = null

  private var trainingData:Dataset[_] = null

  private var modelEntityCache:AtlasEntity = null

  private var modelDirEntityCache:AtlasEntity = null

  private var tableEntityCache:Seq[AtlasEntity] = null

  startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (!shouldContinue) {
      // No op if our tracker is failed to initialize itself
      return
    }

    // We only care about ML related events.
    event match {
      case e: MLListenEvent =>
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
          case CreatePipelineEvent(pipeline, dataset) =>
            pipelineCache = pipeline
            trainingData = dataset

          case CreateModelEvent(pipelineModel) =>
            mlModeCache = pipelineModel

          case SavePipelineEvent(path) =>
            pipelineDirectoryCache = path

          case SaveModelEvent(path) =>
            modelDirectoryCache = path
            val uri = "hdfs://"

            val modelDirEntity = internal.mlDirectoryToEntity(uri, path)
            modelDirEntityCache = modelDirEntity

            val modelEntity = internal.mlModelToEntity(mlModeCache, modelDirEntity)
            modelEntityCache = modelEntity

            atlasClient.createEntities(Seq(modelEntity,modelDirEntity))

            val pipelineDirEntity = internal.mlDirectoryToEntity(uri, path)
            val pipelineEntity = internal.mlPipelineToEntity(pipelineCache, pipelineDirEntity)
            atlasClient.createEntities(Seq(pipelineEntity,pipelineDirEntity))

            //trainingData.createOrReplaceTempView("tmp1")
            val tableEntities1 = getTableEntities("chris1")
            tableEntityCache = tableEntities1

            val fitEntity = internal.mlFitProcessToEntity(
              pipelineCache,
              pipelineEntity,
              List(pipelineEntity, tableEntities1.head),
              List(modelEntity))

            atlasClient.createEntities(Seq(pipelineDirEntity, modelDirEntity,
              pipelineEntity, modelEntity, fitEntity) ++ tableEntities1)

          case TransformEvent(model, dataset) =>

            val tableEntities2 = getTableEntities("chris2")

            val transformEntity = internal.mlTransformProcessToEntity(
              model,
              modelEntityCache,
              List(modelEntityCache, tableEntityCache.head),
              List(tableEntities2.head))

            atlasClient.createEntities(Seq(modelDirEntityCache, modelEntityCache, transformEntity)
              ++ tableEntityCache ++ tableEntities2)
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
