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

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.QueryDetail
import com.hortonworks.spark.atlas.types.external.KAFKA_TOPIC_STRING
import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.sql.streaming.{KafkaHarvester, KafkaTopicInformation}

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.streaming.sources.{MemoryWriterFactory, MicroBatchWriter}
import org.apache.spark.sql.kafka010.{KafkaStreamWriter, KafkaTestUtils}
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, StreamingQuery}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.convert.Wrappers.SeqWrapper
import scala.collection.mutable

class KafkaHarvesterSuite extends StreamTest {
  import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

  val brokerProps = Map[String, Object]()

  val producerParams = Map[String, String]()
  val kafkaWriteSchema = new StructType().add("value", BinaryType)

  var testUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  test("Extract Kafka topic from MicroBatchWriter - no custom atlas cluster name") {
    val topic = Some("hello")

    val writer = new KafkaStreamWriter(topic, producerParams, kafkaWriteSchema)
    val microBatchWriter = new MicroBatchWriter(0L, writer)

    assert(KafkaHarvester.extractTopic(microBatchWriter) ===
      Some(KafkaTopicInformation(topic.get, None)))
  }

  test("Extract Kafka topic from MicroBatchWriter - custom atlas cluster name") {
    val topic = Some("hello")

    val customAtlasClusterName = "newCluster"
    val newProducerParams = producerParams +
      (AtlasClientConf.CLUSTER_NAME.key -> customAtlasClusterName)

    val writer = new KafkaStreamWriter(topic, newProducerParams, kafkaWriteSchema)
    val microBatchWriter = new MicroBatchWriter(0L, writer)

    assert(KafkaHarvester.extractTopic(microBatchWriter) ===
      Some(KafkaTopicInformation(topic.get, Some(customAtlasClusterName))))
  }

  test("No Kafka topic information in WriterFactory") {
    val writer = new FakeStreamWriter()
    val microBatchWriter = new MicroBatchWriter(0L, writer)

    assert(KafkaHarvester.extractTopic(microBatchWriter) === None)
  }

  private class AtlasQueryExecutionListener extends QueryExecutionListener {
    private val executionId = new AtomicLong(0L)
    val queryDetails = new mutable.MutableList[QueryDetail]()

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      queryDetails += QueryDetail(qe, executionId.getAndIncrement(), durationNs)
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
      fail(exception)
    }
  }

  test("Streaming query with Kafka source and sink") {
    def sendMessages(topicsToRead: Seq[String]): Unit = {
      topicsToRead.foreach { topic =>
        testUtils.sendMessages(topic, Array("1", "2", "3", "4", "5"))
      }
    }

    def waitForBatchCompleted(query: StreamingQuery, listener: AtlasQueryExecutionListener)
      : Unit = {
      import org.scalatest.time.SpanSugar._
      eventually(timeout(10.seconds)) {
        query.processAllAvailable()
        assert(listener.queryDetails.nonEmpty)
      }
    }

    def executeHarvest(qd: QueryDetail): Seq[AtlasEntity] = {
      val execAndTopic = qd.qe.sparkPlan.flatMap {
        case r: WriteToDataSourceV2Exec =>
          r.writer match {
            case w: MicroBatchWriter =>
              Seq((r, KafkaHarvester.extractTopic(w)))

            case _ => Nil
          }

        case _ => Nil
      }

      assert(execAndTopic.size == 1)
      val (exec, topic) = execAndTopic.head

      KafkaHarvester.harvest(topic, exec, qd)
    }

    def assertEntitiesKafkaTopicType(topics: Seq[KafkaTopicInformation],
                                     entities: Set[AtlasEntity]): Unit = {
      val kafkaTopicEntities = entities.filter(p => p.getTypeName.equals(KAFKA_TOPIC_STRING))

      val expectedTopicNames = topics.map(_.topicName).toSet
      val expectedClusterNames = topics.map(_.clusterName.getOrElse("primary")).toSet
      val expectedQualifiedNames = topics.map { ti =>
        KafkaTopicInformation.getQualifiedName(ti, "primary")
      }.toSet

      assert(kafkaTopicEntities.size === topics.size)
      assert(kafkaTopicEntities.map(_.getAttribute("name").toString()).toSet === expectedTopicNames)
      assert(kafkaTopicEntities.map(_.getAttribute("topic").toString()).toSet ===
        expectedTopicNames)
      assert(kafkaTopicEntities.map(getStringAttribute(_, "uri")).toSet === expectedTopicNames)
      assert(kafkaTopicEntities.map(getStringAttribute(_, "clusterName")).toSet ===
        expectedClusterNames)
      assert(kafkaTopicEntities.map(getStringAttribute(_, "qualifiedName")).toSet ===
        expectedQualifiedNames)
    }

    def assertEntitySparkProcessType(
        topicsToRead: Seq[KafkaTopicInformation],
        topicToWrite: KafkaTopicInformation,
        entities: Set[AtlasEntity],
        queryDetails: Seq[QueryDetail]): Unit = {
      val processEntities = entities.filter { p =>
        p.getTypeName.equals(metadata.PROCESS_TYPE_STRING)
      }

      assert(processEntities.size === 1)
      val processEntity = processEntities.head

      val inputs = processEntity.getAttribute("inputs")
        .asInstanceOf[SeqWrapper[AtlasEntity]].underlying
      val outputs = processEntity.getAttribute("outputs")
        .asInstanceOf[SeqWrapper[AtlasEntity]].underlying

      assert(!inputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))
      assert(!outputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))

      // unfortunately each batch recognizes topics which topics are having records to process
      // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
      assert(inputs.map(getStringAttribute(_, "qualifiedName")).toSet.subsetOf(
        topicsToRead.map(KafkaTopicInformation.getQualifiedName(_, "primary")).toSet))

      assert(outputs.map(getStringAttribute(_, "qualifiedName")).toSet ===
        Seq(topicToWrite).map(KafkaTopicInformation.getQualifiedName(_, "primary")).toSet)

      // verify others
      // it is OK if there's a matching query detail: since only one is exactly
      // matched to 'spark_process' entity
      val anyMatchingFound = queryDetails.exists { queryDetail =>
        val expectedMap = Map(
          "executionId" -> queryDetail.executionId.toString,
          "remoteUser" -> SparkUtils.currSessionUser(queryDetail.qe),
          "executionTime" -> queryDetail.executionTime.toString,
          "details" -> queryDetail.qe.toString()
        )

        expectedMap.forall { case (key, value) =>
          processEntity.getAttribute(key) == value
        }
      }

      assert(anyMatchingFound)
    }

    val topicsToRead = Seq("sparkread1", "sparkread2", "sparkread3")
    val clusterNameForSources = "source"

    val topicToWrite = "sparkwrite"
    val clusterNameForSink = "sink"

    val topics = topicsToRead :+ topicToWrite

    val brokerAddress = testUtils.brokerAddress

    topics.foreach(testUtils.createTopic(_, 10, overwrite = true))

    val listener = new AtlasQueryExecutionListener
    spark.listenerManager.register(listener)

    val tempDir = Files.createTempDirectory("spark-atlas-kafka-harvester")

    val topicsToReadWithClusterInfo = topicsToRead.map { tp =>
      KafkaTopicInformation(tp, Some(clusterNameForSources))
    }
    val topicToWriteWithClusterInfo = KafkaTopicInformation(topicToWrite, Some(clusterNameForSink))

    val topicsWithClusterInfo = topicsToReadWithClusterInfo ++ Seq(topicToWriteWithClusterInfo)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, clusterNameForSources)
      .option("subscribe", topicsToRead.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    val query = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, clusterNameForSink)
      .option("topic", topicToWrite)
      .option("checkpointLocation", tempDir.toAbsolutePath.toString)
      .start()

    // remove temporary directory in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          Files.deleteIfExists(tempDir)
        }
      }, 10)

    try {
      sendMessages(topicsToRead)
      waitForBatchCompleted(query, listener)

      import org.scalatest.time.SpanSugar._
      var queryDetails: Seq[QueryDetail] = null
      var entitySet: Set[AtlasEntity] = null
      eventually(timeout(10.seconds)) {
        queryDetails = listener.queryDetails

        val atlasEntities = queryDetails.flatMap(executeHarvest)
        logInfo(s"Count of created entities (with duplication): ${atlasEntities.size}")
        entitySet = getUniqueEntities(atlasEntities)
        logInfo(s"Count of created entities after deduplication: ${entitySet.size}")

        // spark_process, topic to write, topics to read
        assert(entitySet.size == topicsToRead.size + 2)
      }

      assertEntitiesKafkaTopicType(topicsWithClusterInfo, entitySet)
      assertEntitySparkProcessType(topicsToReadWithClusterInfo, topicToWriteWithClusterInfo,
        entitySet, queryDetails)
    } finally {
      query.stop()
    }
  }

  private def getUniqueEntities(entities: Seq[AtlasEntity]): Set[AtlasEntity] = {
    // same entities must be taken only once, and it is not likely to be done with equals
    // because pseudo guid is generated per each creation and 'equals' checks this value
    // so we take 'typeName' and 'qualifiedName' as a unique qualifier

    // (type, qualifier) -> AtlasEntity first occurred
    val entitiesMap = new scala.collection.mutable.HashMap[(String, String), AtlasEntity]()

    entities.foreach { entity =>
      val typeName = entity.getTypeName
      val qualifiedName = getStringAttribute(entity, "qualifiedName")
      val mapKey = (typeName, qualifiedName)
      if (!entitiesMap.contains(mapKey)) {
        entitiesMap.put(mapKey, entity)
      }
    }

    entitiesMap.values.toSet
  }

  private class FakeStreamWriter extends StreamWriter {
    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

    override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

    override def createWriterFactory(): DataWriterFactory[InternalRow] = {
      MemoryWriterFactory(OutputMode.Append(), kafkaWriteSchema)
    }
  }

}
