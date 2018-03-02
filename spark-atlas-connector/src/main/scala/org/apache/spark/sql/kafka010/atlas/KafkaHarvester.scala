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
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.kafka010.{KafkaSourceRDDPartition, KafkaStreamWriter}

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external}
import com.hortonworks.spark.atlas.utils.Logging

object KafkaHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  def harvest(node: KafkaStreamWriter, writer: WriteToDataSourceV2Exec,
    qd: QueryDetail) : Seq[AtlasEntity] = {
    // source topic
    val tChildren = writer.query.collectLeaves()
    val inputsEntities = tChildren.flatMap{
      case r: RDDScanExec =>
        r.rdd.partitions.map {
          case e: KafkaSourceRDDPartition =>
            println("-----KafkaSourceRDDPartition -------")
            external.kafkaToEntity(clusterName, e.offsetRange.topic)
        }.toSeq.flatten
    }

    // destination topic
    var topic = None: Option[String]
    try {
      topic = node.getClass.getMethod("topic").invoke(node).asInstanceOf[Option[String]]
    } catch {
      case e: NoSuchMethodException =>
        println("-----KafkaHarvester NoSuchMethodException-------" + node.getClass)
        println(s"Can not get topic, so can not create Kafka topic entities: ${qd.qe}")
        logDebug(s"Can not get topic, so can not create Kafka topic entities: ${qd.qe}")
    }
    val outputEntities = if (topic.isDefined) external.kafkaToEntity(clusterName, topic.get) else Seq.empty

    // create process entity
    val inputTablesEntities = inputsEntities.toList
    val outputTableEntities = outputEntities.toList
    val pEntity = processToEntity(
      qd.qe, qd.executionId, qd.executionTime, inputTablesEntities, outputTableEntities)
    Seq(pEntity) ++ inputsEntities ++ outputEntities
  }

}
