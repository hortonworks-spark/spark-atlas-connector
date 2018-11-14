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
import org.apache.spark.sql.kafka010.{KafkaSourceRDDPartition, KafkaStreamWriter, KafkaStreamWriterFactory}
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter

object KafkaHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  def extractTopic(writer: KafkaStreamWriter): Option[String] = {
    // Unfortunately neither KafkaStreamWriter is a case class nor topic is a field.
    // Hopefully KafkaStreamWriterFactory is a case class instead, so we can extract
    // topic information from there.
    // The cost of createWriterFactory is tiny (case class object creation) for this case,
    // and we can find the way to cache it once we find the cost is not ignorable.
    writer.createInternalRowWriterFactory() match {

      case   KafkaStreamWriterFactory(tp, _, _) => tp
      case _ => None
    }
  }

  def harvest(targetTopic: Option[String], writer: WriteToDataSourceV2Exec,
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

    val outputEntities = if (targetTopic.isDefined) {
      external.kafkaToEntity(clusterName, targetTopic.get)
    } else {
      Seq.empty
    }

    // create process entity
    val pDescription = StringBuilder.newBuilder.append("Topics subscribed( ")
    read_from_topics.sorted.flatMap{
      e => pDescription.append(e).append(" ")
    }

    if (targetTopic.isDefined) {
      pDescription.append(") Topics written into( ").append(targetTopic.get).append(" )")
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
