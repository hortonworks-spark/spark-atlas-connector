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

package com.hortonworks.spark.atlas.sql.streaming

import scala.collection.mutable

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.kafka010._
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.kafka010.atlas.ExtractFromDataSource

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}


case class KafkaTopicInformation(topicName: String, clusterName: Option[String] = None)

object KafkaTopicInformation {
  def getQualifiedName(ti: KafkaTopicInformation, defaultClusterName: String): String = {
    val cName = ti.clusterName.getOrElse(defaultClusterName)
    s"${ti.topicName}@$cName"
  }
}

object KafkaHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  // reflection
  import scala.reflect.runtime.universe.{TermName, runtimeMirror, typeOf}
  private val currentMirror = runtimeMirror(getClass.getClassLoader)

  def extractTopic(writer: MicroBatchWriter): Option[KafkaTopicInformation] = {
    // Unfortunately neither KafkaStreamWriter is a case class nor topic is a field.
    // Hopefully KafkaStreamWriterFactory is a case class instead, so we can extract
    // topic information from there, as well as producer parameters.
    // The cost of createWriterFactory is tiny (case class object creation) for this case,
    // and we can find the way to cache it once we find the cost is not ignorable.
    writer.createWriterFactory() match {
      case KafkaStreamWriterFactory(Some(tp), params, _) =>
        Some(KafkaTopicInformation(tp, params.get(AtlasClientConf.CLUSTER_NAME.key)))
      case _ => None
    }
  }

  def harvest(
      targetTopic: Option[KafkaTopicInformation],
      writer: WriteToDataSourceV2Exec,
      qd: QueryDetail) : Seq[AtlasEntity] = {
    // source topics - can be multiple topics
    val sourceTopics = extractSourceTopics(writer)
    val inputsEntities: Seq[AtlasEntity] = extractInputEntities(sourceTopics)

    val outputEntities = if (targetTopic.isDefined) {
      external.kafkaToEntity(clusterName, targetTopic.get)
    } else {
      Seq.empty
    }

    val logMap = makeLogMap(sourceTopics, targetTopic, qd)
    makeProcessEntities(inputsEntities, outputEntities, logMap)
  }

  def extractSourceTopics(node: WriteToDataSourceV2Exec): Set[KafkaTopicInformation] = {
    node.query.collectLeaves().flatMap {
      case r: RDDScanExec => ExtractFromDataSource.extractSourceTopicsFromDataSourceV1(r)
      case r: DataSourceV2ScanExec => ExtractFromDataSource.extractSourceTopicsFromDataSourceV2(r)
      case _ => Nil
    }.toSet
  }

  def extractInputEntities(
       sourceTopics: Set[KafkaTopicInformation]): Seq[AtlasEntity] = {
    sourceTopics
      .toList.flatMap { topic => external.kafkaToEntity(clusterName, topic) }
  }

  def makeLogMap(
      sourceTopics: Set[KafkaTopicInformation],
      targetTopic: Option[KafkaTopicInformation],
      qd: QueryDetail): Map[String, String] = {
    // create process entity
    val strSourceTopics = sourceTopics.toList
      .map(KafkaTopicInformation.getQualifiedName(_, clusterName)).sorted.mkString(", ")

    val pDescription = StringBuilder.newBuilder.append(s"Topics subscribed( $strSourceTopics )")

    if (targetTopic.isDefined) {
      val strTargetTopic = KafkaTopicInformation.getQualifiedName(targetTopic.get, clusterName)
      pDescription.append(s" Topics written into( $strTargetTopic )")
    } else {
      logInfo(s"Can not get dest topic")
    }

    Map(
      "executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "executionTime" -> qd.executionTime.toString,
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> pDescription.toString())
  }

  def makeProcessEntities(
      inputsEntities: Seq[AtlasEntity],
      outputEntities: Seq[AtlasEntity],
      logMap: Map[String, String]): Seq[AtlasEntity] = {
    val inputTablesEntities = inputsEntities.toList
    val outputTableEntities = outputEntities.toList

    // ml related cached object
    if (internal.cachedObjects.contains("model_uid")) {

      internal.updateMLProcessToEntity(inputTablesEntities, outputEntities, logMap)

    } else {

      // create process entity
      val pEntity = internal.etlProcessToEntity(
        inputTablesEntities, outputTableEntities, logMap)

      Seq(pEntity) ++ inputsEntities ++ outputEntities
    }
  }
}
