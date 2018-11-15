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

package org.apache.spark.sql.kafka010.atlas

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.kafka010.{KafkaContinuousInputPartition, KafkaMicroBatchInputPartition, KafkaSourceRDDPartition, KafkaStreamWriterFactory}
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter

import scala.collection.mutable

object KafkaHarvester extends AtlasEntityUtils with Logging {
  val CONFIG_CUSTOM_CLUSTER_NAME: String = "atlas.cluster.name"

  override val conf: AtlasClientConf = new AtlasClientConf

  def extractTopic(writer: MicroBatchWriter): Option[KafkaTopicInformation] = {
    // Unfortunately neither KafkaStreamWriter is a case class nor topic is a field.
    // Hopefully KafkaStreamWriterFactory is a case class instead, so we can extract
    // topic information from there, as well as producer parameters.
    // The cost of createWriterFactory is tiny (case class object creation) for this case,
    // and we can find the way to cache it once we find the cost is not ignorable.
    writer.createWriterFactory() match {
      case KafkaStreamWriterFactory(Some(tp), params, _) =>
        Some(KafkaTopicInformation(tp, params.get(CONFIG_CUSTOM_CLUSTER_NAME)))
      case _ => None
    }
  }

  def harvest(
      targetTopic: Option[KafkaTopicInformation],
      writer: WriteToDataSourceV2Exec,
      qd: QueryDetail) : Seq[AtlasEntity] = {
    // source topics - can be multiple topics
    val tChildren = writer.query.collectLeaves()
    val sourceTopics: Set[KafkaTopicInformation] = tChildren.flatMap {
      case r: RDDScanExec => extractSourceTopicsFromDataSourceV1(r)
      case r: DataSourceV2ScanExec => extractSourceTopicsFromDataSourceV2(r)
      case _ => Nil
    }.toSet

    val inputsEntities: Seq[AtlasEntity] = sourceTopics.toList.flatMap { topic =>
      external.kafkaToEntity(clusterName, topic)
    }

    val outputEntities = if (targetTopic.isDefined) {
      external.kafkaToEntity(clusterName, targetTopic.get)
    } else {
      Seq.empty
    }

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

    val inputTablesEntities = inputsEntities.toList
    val outputTableEntities = outputEntities.toList

    val logMap = Map(
      "executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "executionTime" -> qd.executionTime.toString,
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> pDescription.toString())

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

  private def extractSourceTopicsFromDataSourceV1(r: RDDScanExec): Seq[KafkaTopicInformation] = {
    val topics = new mutable.HashSet[KafkaTopicInformation]()
    r.rdd.partitions.foreach {
      case e: KafkaSourceRDDPartition =>
        val topic = e.offsetRange.topic
        topics += KafkaTopicInformation(topic, None)
    }
    topics.toSeq
  }

  private def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExec)
    : Seq[KafkaTopicInformation] = {
    val topics = new mutable.HashSet[KafkaTopicInformation]()
    r.inputRDDs().foreach(rdd => rdd.partitions.foreach {
      case e: DataSourceRDDPartition[_] =>
        e.inputPartition match {
          case e1: KafkaMicroBatchInputPartition =>
            val topic = e1.offsetRange.topicPartition.topic()
            val customClusterName = e1.executorKafkaParams.get(CONFIG_CUSTOM_CLUSTER_NAME)
              .asInstanceOf[String]
            topics += KafkaTopicInformation(topic, Option(customClusterName))

          case e1: KafkaContinuousInputPartition =>
            val topic = e1.topicPartition.topic()
            val customClusterName = e1.kafkaParams.get(CONFIG_CUSTOM_CLUSTER_NAME)
              .asInstanceOf[String]
            topics += KafkaTopicInformation(topic, Option(customClusterName))

          case _ =>
        }

      case _ =>
    })

    topics.toSeq
  }
}
