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

import scala.collection.mutable
import scala.util.control.NonFatal

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.streaming.KafkaTopicInformation
import com.hortonworks.spark.atlas.utils.Logging

import org.apache.spark.sql.execution.RDDScanExec
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanExec}
import org.apache.spark.sql.kafka010._


/**
 * An object that defines an method that extracts `KafkaTopicInformation` from data source plans
 * This is located under `org.apache.spark.sql.kafka010.atlas` on purpose
 * to access to package level classes such as `KafkaSourceRDD` from Apache Spark.
 */
object ExtractFromDataSource extends Logging {
  import scala.reflect.runtime.universe.{TermName, runtimeMirror, typeOf}
  private val currentMirror = runtimeMirror(getClass.getClassLoader)

  def extractSourceTopicsFromDataSourceV1(r: RDDScanExec): Seq[KafkaTopicInformation] = {
    def extractKafkaParams(rdd: KafkaSourceRDD): Option[java.util.Map[String, Object]] = {
      val rddMirror = currentMirror.reflect(rdd)

      try {
        val kafkaParamsMethod = typeOf[KafkaSourceRDD].decl(TermName("executorKafkaParams"))
          .asTerm.accessed.asTerm

        Some(rddMirror.reflectField(kafkaParamsMethod).get
          .asInstanceOf[java.util.Map[String, Object]])
      } catch {
        case NonFatal(_) =>
          logWarn("WARN: Necessary patch for spark-sql-kafka doesn't look like applied to Spark. " +
            "Giving up extracting kafka parameter.")
          None
      }
    }

    val topics = new mutable.HashSet[KafkaTopicInformation]()
    r.rdd.partitions.foreach {
      case e: KafkaSourceRDDPartition =>
        r.rdd.dependencies.find(p => p.rdd.isInstanceOf[KafkaSourceRDD]).map(_.rdd) match {
          case Some(kafkaRDD: KafkaSourceRDD) =>
            val topic = e.offsetRange.topic
            val customClusterName = extractKafkaParams(kafkaRDD) match {
              case Some(params) => Option(params.get(AtlasClientConf.CLUSTER_NAME.key))
                .map(_.toString)
              case None => None
            }
            topics += KafkaTopicInformation(topic, customClusterName)

          case _ =>
            topics += KafkaTopicInformation(e.offsetRange.topic, None)
        }

      case _ =>
    }
    topics.toSeq
  }

  def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExec): Seq[KafkaTopicInformation] = {
    val topics = new mutable.HashSet[KafkaTopicInformation]()
    r.inputRDDs().foreach(rdd => rdd.partitions.foreach {
      case e: DataSourceRDDPartition[_] =>
        e.inputPartition match {
          case e1: KafkaMicroBatchInputPartition =>
            val topic = e1.offsetRange.topicPartition.topic()
            val customClusterName = e1.executorKafkaParams.get(AtlasClientConf.CLUSTER_NAME.key)
              .asInstanceOf[String]
            topics += KafkaTopicInformation(topic, Option(customClusterName))

          case e1: KafkaContinuousInputPartition =>
            val topic = e1.topicPartition.topic()
            val customClusterName = e1.kafkaParams.get(AtlasClientConf.CLUSTER_NAME.key)
              .asInstanceOf[String]
            topics += KafkaTopicInformation(topic, Option(customClusterName))

          case _ =>
        }

      case _ =>
    })

    topics.toSeq
  }
}
