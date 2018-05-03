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

import scala.collection.mutable.ListBuffer
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.kafka010.{KafkaSourceRDDPartition, KafkaStreamWriter}
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object KafkaHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  def harvest(node: KafkaStreamWriter, writer: WriteToDataSourceV2Exec,
    qd: QueryDetail) : Seq[AtlasEntity] = {
    // source topics - can be multiple topics
    val read_from_topics = new ListBuffer[String]()
    val tChildren = writer.query.collectLeaves()
    val inputsEntities = tChildren.flatMap{
      case r: RDDScanExec =>
        r.rdd.partitions.map {
          case e: KafkaSourceRDDPartition =>
            val topic = e.offsetRange.topic
            read_from_topics += topic
            external.kafkaToEntity(clusterName, topic)
        }.toSeq.flatten
    }

    // destination topic
    var destTopic = None: Option[String]
    try {
      val topicField = node.getClass.getDeclaredField("topic")
      topicField.setAccessible(true)
      destTopic = topicField.get(node).asInstanceOf[Option[String]]
    } catch {
      case e: NoSuchMethodException =>
        logDebug(s"Can not get topic, so can not create Kafka topic entities: ${qd.qe}")
      case e: Exception =>
        logDebug(s"Can not get topic, please update topic of KafkaStreamWriter: ${e.toString}")
    }

    val outputEntities = if (destTopic.isDefined) {
      external.kafkaToEntity(clusterName, destTopic.get)
    } else {
      Seq.empty
    }

    // create process entity
    val pDescription = StringBuilder.newBuilder.append("Topics subscribed( ")
    read_from_topics.sorted.flatMap{
      e => pDescription.append(e).append(" ")
    }

    if(destTopic.isDefined) {
      pDescription.append(") Topics written into( ").append(destTopic.get).append(" )")
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
}
