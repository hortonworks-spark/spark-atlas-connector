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
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanExec}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.types.StructType
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.KafkaTopicInformation
import com.hortonworks.spark.atlas.utils.{Logging, ReflectionHelper}
import org.apache.spark.sql.sources.v2.DataSourceV2

/**
 * An object that defines an method that extracts `KafkaTopicInformation` from data source plans
 * This is located under `org.apache.spark.sql.kafka010.atlas` on purpose
 * to access to package level classes such as `KafkaSourceRDD` from Apache Spark.
 *
 * Hack alert: This class is implemented with whole of reflections on accessing
 * spark-sql-kafka module, which might not be loaded into same classloader on SAC classes.
 * E.g. If SAC jar is linked from "spark.driver.extraClassPath" and spark-sql-kafka module is
 * dynamically loaded via "--jars" or "--packages" option of spark-submit/spark-shell.
 */
object ExtractFromDataSource extends Logging {
  private val CLASS_NAME_KAFKA_STREAM_WRITER_FACTORY =
    "org.apache.spark.sql.kafka010.KafkaStreamWriterFactory"
  private val CLASS_NAME_KAFKA_SOURCE_RDD_PARTITION =
    "org.apache.spark.sql.kafka010.KafkaSourceRDDPartition"
  private val CLASS_NAME_KAFKA_OFFSET_RANGE = "org.apache.spark.sql.kafka010.KafkaOffsetRange"
  private val CLASS_NAME_TOPIC_PARTITION = "org.apache.kafka.common.TopicPartition"
  private val CLASS_NAME_KAFKA_SOURCE_RDD = "org.apache.spark.sql.kafka010.KafkaSourceRDD"
  private val CLASS_NAME_KAFKA_RELATION = "org.apache.spark.sql.kafka010.KafkaRelation"
  private val CLASS_NAME_KAFKA_SOURCE_PROVIDER = "org.apache.spark.sql.kafka010.KafkaSourceProvider"
  private val CLASS_NAME_KAFKA_MICRO_BATCH_INPUT_PARTITION =
    "org.apache.spark.sql.kafka010.KafkaMicroBatchInputPartition"
  private val CLASS_NAME_KAFKA_CONTINUOUS_INPUT_PARTITION =
    "org.apache.spark.sql.kafka010.KafkaContinuousInputPartition"

  def extractTopic(writer: MicroBatchWriter): Option[KafkaTopicInformation] = {
    // Unfortunately neither KafkaStreamWriter is a case class nor topic is a field.
    // Hopefully KafkaStreamWriterFactory is a case class instead, so we can extract
    // topic information from there, as well as producer parameters.
    // The cost of createWriterFactory is tiny (case class object creation) for this case,
    // and we can find the way to cache it once we find the cost is not ignorable.
    val writerFactory = writer.createWriterFactory()
    populateValuesFromKafkaStreamWriterFactory(writerFactory) match {
      case Some((Some(tp), params, _)) =>
        Some(KafkaTopicInformation(tp, params.get(AtlasClientConf.CLUSTER_NAME.key)))

      case _ => None
    }
  }

  def extractSourceTopicsFromDataSourceV1(r: RDDScanExec): Seq[KafkaTopicInformation] = {
    extractSourceTopicsFromDataSourceV1(r.rdd)
  }

  def extractSourceTopicsFromDataSourceV1(r: RowDataSourceScanExec): Seq[KafkaTopicInformation] = {
    extractSourceTopicsFromDataSourceV1(r.rdd)
  }

  def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExec): Seq[KafkaTopicInformation] = {
    val topics = new mutable.HashSet[KafkaTopicInformation]()
    r.inputRDDs().foreach(rdd => rdd.partitions.foreach {
      case e: DataSourceRDDPartition[_] =>
        if (isKafkaMicroBatchInputPartition(e.inputPartition)) {
          populateValuesFromKafkaMicroBatchInputPartition(e.inputPartition) match {
            case Some((topic, customClusterName)) =>
              topics += KafkaTopicInformation(topic, customClusterName)

            case _ =>
          }
        } else if (isKafkaContinuousInputPartition(e.inputPartition)) {
          populateValuesFromKafkaContinuousInputPartition(e.inputPartition) match {
            case Some((topic, customClusterName)) =>
              topics += KafkaTopicInformation(topic, customClusterName)

            case _ =>
          }
        }

      case _ =>
    })

    topics.toSeq
  }

  def extractSourceTopicsFromKafkaSourceRDDPartition(
      e: Partition,
      rddContainingPartition: RDD[_]): Set[KafkaTopicInformation] = {
    def collectLeaves(rdd: RDD[_]): Seq[RDD[_]] = {
      // this method is being called with chains of MapPartitionRDDs
      // so this recursion won't stack up too much
      if (rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(collectLeaves)
      }
    }

    val topics = new mutable.HashSet[KafkaTopicInformation]()
    val rdds = collectLeaves(rddContainingPartition)
    extractSourceTopicFromKafkaSourceRDDPartition(e) match {
      case Some(topic) =>
        rdds.find(isKafkaSourceRdd) match {
          case Some(rdd) =>
            val customClusterName = extractKafkaParamsFromKafkaSourceRDD(rdd) match {
              case Some(params) => Option(params.get(AtlasClientConf.CLUSTER_NAME.key))
                .map(_.toString)
              case None => None
            }
            topics += KafkaTopicInformation(topic, customClusterName)

          case _ =>
            topics += KafkaTopicInformation(topic, None)
        }

      case None => // give-up, as reflection didn't help, unfortunately
    }

    topics.toSet
  }

  def extractSourceTopicsFromKafkaRelation(rel: BaseRelation): Set[KafkaTopicInformation] = {
    if (isKafkaRelation(rel)) {
      ReflectionHelper.reflectFieldWithContextClassloader[Map[String, String]](
        rel, "sourceOptions") match {
        case Some(sourceOptions) =>
          val topics = if (sourceOptions.contains("subscribe")) {
            sourceOptions("subscribe").split(",").toSeq
          } else if (sourceOptions.contains("assign")) {
            topicsFromSourceOptionAssign(sourceOptions("assign")).toSeq
          } else {
            // it should be 'subscribePattern' which we can't get information - give up
            Seq.empty[String]
          }

          val customClusterName = sourceOptions.get("kafka." + AtlasClientConf.CLUSTER_NAME.key)
            .map(_.toString)

          topics.map(KafkaTopicInformation(_, customClusterName)).toSet

        case None => Nil.toSet
      }
    } else {
      Nil.toSet
    }
  }

  def isKafkaRelation(rel: BaseRelation): Boolean = {
    val kafkaClazz = ReflectionHelper.classForName(CLASS_NAME_KAFKA_RELATION)
    kafkaClazz.isAssignableFrom(rel.getClass)
  }

  def isKafkaRelationProvider(source: DataSourceV2): Boolean = {
    val kafkaClazz = ReflectionHelper.classForName(CLASS_NAME_KAFKA_SOURCE_PROVIDER)
    kafkaClazz.isAssignableFrom(source.getClass)
  }

  def isKafkaRelationProvider(provider: CreatableRelationProvider): Boolean = {
    val kafkaClazz = ReflectionHelper.classForName(CLASS_NAME_KAFKA_SOURCE_PROVIDER)
    kafkaClazz.isAssignableFrom(provider.getClass)
  }

  def isKafkaSourceRdd(rdd: RDD[_]): Boolean = {
    val kafkaClazz = ReflectionHelper.classForName(CLASS_NAME_KAFKA_SOURCE_RDD)
    kafkaClazz.isAssignableFrom(rdd.getClass)
  }

  private def isKafkaStreamWriterFactory(writer: DataWriterFactory[InternalRow]): Boolean = {
    belongToTargetClass(CLASS_NAME_KAFKA_STREAM_WRITER_FACTORY, writer)
  }

  private def isKafkaSourceRddPartition(p: Partition): Boolean = {
    belongToTargetClass(CLASS_NAME_KAFKA_SOURCE_RDD_PARTITION, p)
  }

  private def isKafkaOffsetRange(offsetRange: Any): Boolean = {
    belongToTargetClass(CLASS_NAME_KAFKA_OFFSET_RANGE, offsetRange)
  }

  private def isKafkaTopicPartition(topicPartition: Any): Boolean = {
    belongToTargetClass(CLASS_NAME_TOPIC_PARTITION, topicPartition)
  }

  private def isKafkaMicroBatchInputPartition(p: InputPartition[_]): Boolean = {
    belongToTargetClass(CLASS_NAME_KAFKA_MICRO_BATCH_INPUT_PARTITION, p)
  }

  private def isKafkaContinuousInputPartition(p: InputPartition[_]): Boolean = {
    belongToTargetClass(CLASS_NAME_KAFKA_CONTINUOUS_INPUT_PARTITION, p)
  }

  private def extractSourceTopicsFromDataSourceV1(
      rddContainingPartition: RDD[_]): Seq[KafkaTopicInformation] = {
    rddContainingPartition.partitions.flatMap { p =>
      if (isKafkaSourceRddPartition(p)) {
        extractSourceTopicsFromKafkaSourceRDDPartition(p, rddContainingPartition)
      } else {
        Nil
      }
    }
  }

  private def populateValuesFromKafkaStreamWriterFactory(
      writer: DataWriterFactory[InternalRow])
    : Option[(Option[String], Map[String, String], StructType)] = {

    if (isKafkaStreamWriterFactory(writer)) {
      val topic = ReflectionHelper.reflectFieldWithContextClassloader[Option[String]](
        writer, "topic")
      val params = ReflectionHelper
        .reflectFieldWithContextClassloader[Map[String, String]](writer, "producerParams")
      val schema = ReflectionHelper.reflectFieldWithContextClassloader[StructType](writer, "schema")

      (topic, params, schema) match {
        case (Some(Some(t)), Some(p), Some(s)) => Some((Some(t), p, s))
        case _ => None
      }
    } else {
      None
    }
  }

  private def extractSourceTopicFromKafkaSourceRDDPartition(p: Partition): Option[String] = {
    if (isKafkaSourceRddPartition(p)) {
      ReflectionHelper.reflectFieldWithContextClassloaderLoosenType(p,
        "offsetRange") match {
        case Some(offsetRange) =>
          ReflectionHelper.reflectMethodWithContextClassloader[String](offsetRange, "topic")

        case _ => None
      }
    } else {
      None
    }
  }

  // Sadly KafkaOffsetRange is case class which doesn't extend anything outside of spark-sql-kafka
  // so we can't refer it to any class.
  // Same as TopicPartition (in Kafka-clients) we need to access inside KafkaOffsetRange,
  // so we have to call them without any type information.
  private def extractSourceTopicFromKafkaOffsetRange(obj: Any): Option[String] = {
    if (isKafkaOffsetRange(obj)) {
      ReflectionHelper.reflectFieldWithContextClassloaderLoosenType(obj, "topicPartition") match {
        case Some(part) => extractSourceTopicFromTopicPartition(part)
        case None => None
      }
    } else {
      None
    }
  }

  private def extractSourceTopicFromTopicPartition(partition: Any): Option[String] = {
    if (isKafkaTopicPartition(partition)) {
      ReflectionHelper.reflectMethodWithContextClassloader[String](partition, "topic")
    } else {
      None
    }
  }

  private def extractKafkaParamsFromKafkaSourceRDD(
      rdd: RDD[_]): Option[java.util.Map[String, Object]] = {
    if (isKafkaSourceRdd(rdd)) {
      ReflectionHelper.reflectFieldWithContextClassloader[java.util.Map[String, Object]](
        rdd, "executorKafkaParams")
    } else {
      None
    }
  }

  private def populateValuesFromKafkaMicroBatchInputPartition(
      p: InputPartition[_]): Option[(String, Option[String])] = {
    if (isKafkaMicroBatchInputPartition(p)) {
      val topic = ReflectionHelper.reflectFieldWithContextClassloaderLoosenType(
        p, "offsetRange") match {
        case Some(offsetRange) => extractSourceTopicFromKafkaOffsetRange(offsetRange)
        case None => None
      }

      val customClusterName = ReflectionHelper.reflectFieldWithContextClassloader[
        java.util.Map[String, Object]](p, "executorKafkaParams") match {
        case Some(params) =>
          Option(params.get(AtlasClientConf.CLUSTER_NAME.key).asInstanceOf[String])
        case None => None
      }

      if (topic.isDefined) {
        Some((topic.get, customClusterName))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def populateValuesFromKafkaContinuousInputPartition(
      p: InputPartition[_]): Option[(String, Option[String])] = {
    if (isKafkaContinuousInputPartition(p)) {
      val topic = ReflectionHelper.reflectFieldWithContextClassloaderLoosenType(
        p, "topicPartition") match {
        case Some(topicPartition) => extractSourceTopicFromTopicPartition(topicPartition)
        case None => None
      }

      val customClusterName = ReflectionHelper.reflectFieldWithContextClassloader[
        java.util.Map[String, Object]](p, "kafkaParams") match {
        case Some(params) =>
          Option(params.get(AtlasClientConf.CLUSTER_NAME.key).asInstanceOf[String])
        case None => None
      }

      if (topic.isDefined) {
        Some((topic.get, customClusterName))
      } else {
        None
      }
    } else {
      None
    }
  }

  private implicit val formats = Serialization.formats(NoTypeHints)

  private def topicsFromSourceOptionAssign(str: String): Set[String] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).map { case (topic, _) => topic }.toSet
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  private def belongToTargetClass(clazzName: String, obj: Any): Boolean = {
    val targetClass = ReflectionHelper.classForName(clazzName)
    targetClass == obj.getClass
  }
}
