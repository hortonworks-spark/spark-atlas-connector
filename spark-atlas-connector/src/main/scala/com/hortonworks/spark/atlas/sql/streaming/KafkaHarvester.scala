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

import scala.util.Try

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.execution.{FileSourceScanExec, RDDScanExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.InternalRowMicroBatchWriter
import org.apache.spark.sql.kafka010.atlas.ExtractFromDataSource
import org.apache.spark.sql.kafka010.KafkaStreamWriterFactory

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.{CommandsHarvester, QueryDetail}
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

  def extractTopic(writer: InternalRowMicroBatchWriter)
    : (Boolean, Option[KafkaTopicInformation]) = {
    // Unfortunately neither KafkaStreamWriter is a case class nor topic is a field.
    // Hopefully KafkaStreamWriterFactory is a case class instead, so we can extract
    // topic information from there.
    // The cost of createInternalRowWriterFactory is tiny (case class object creation)
    // for this case, and we can find the way to cache it once we find the cost is not ignorable.
    writer.createInternalRowWriterFactory() match {
      case KafkaStreamWriterFactory(Some(tp), params, _) =>
        (true, Some(KafkaTopicInformation(tp, params.get(AtlasClientConf.CLUSTER_NAME.key))))
      case _ => (false, None)
    }
  }

  def harvest(
      targetTopic: Option[KafkaTopicInformation],
      writer: WriteToDataSourceV2Exec,
      qd: QueryDetail) : Seq[AtlasEntity] = {
    // source topics - can be multiple topics
    val inputsEntities: Seq[AtlasEntity] = extractInputEntities(writer)

    val outputEntities = if (targetTopic.isDefined) {
      external.kafkaToEntity(clusterName, targetTopic.get)
    } else {
      logInfo(s"Could not get destination topic.")
      Seq.empty
    }

    val logMap = makeLogMap(writer, targetTopic, qd)
    makeProcessEntities(inputsEntities, outputEntities, logMap)
  }

  private def extractSourceTopics(node: WriteToDataSourceV2Exec): Set[KafkaTopicInformation] = {
    node.query.flatMap {
      case r: RDDScanExec => ExtractFromDataSource.extractSourceTopicsFromDataSourceV1(r)
      case r: DataSourceV2ScanExec => ExtractFromDataSource.extractSourceTopicsFromDataSourceV2(r)
      case _ => Nil
    }.toSet
  }

  def extractInputEntities(node: WriteToDataSourceV2Exec): Seq[AtlasEntity] = {
    val kafkaInputEntities = extractSourceTopics(node).toList
      .flatMap { topic => external.kafkaToEntity(clusterName, topic) }

    val otherInputEntities = node.query.collectLeaves().flatMap {
      case f: FileSourceScanExec =>
        f.tableIdentifier.map(CommandsHarvester.prepareEntities).getOrElse(
          f.relation.location.inputFiles.map(external.pathToEntity).toSeq)
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }

    kafkaInputEntities ++ otherInputEntities
  }

  def makeLogMap(
      node: WriteToDataSourceV2Exec,
      targetTopic: Option[KafkaTopicInformation],
      qd: QueryDetail): Map[String, String] = {
    // create process entity
    val sourceTopics = extractSourceTopics(node).toList
    val pDescription = StringBuilder.newBuilder
    if (sourceTopics.nonEmpty) {
      val strSourceTopics = sourceTopics.map(
        KafkaTopicInformation.getQualifiedName(_, clusterName)).sorted.mkString(", ")
      pDescription.append(s"Topics subscribed( $strSourceTopics )")
    }
    if (targetTopic.isDefined) {
      val strTargetTopic = KafkaTopicInformation.getQualifiedName(targetTopic.get, clusterName)
      pDescription.append(s" Topics written into( $strTargetTopic )")
    }

    pDescription.append(s"\n${qd.qe.sparkPlan.toString()}")

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
