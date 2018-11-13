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

package com.hortonworks.spark.atlas.sql

import java.nio.file.Files

import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor}
import com.hortonworks.spark.atlas.types.external.KAFKA_TOPIC_STRING
import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.AtlasClientConf
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery}

import scala.collection.convert.Wrappers.SeqWrapper

class SparkExecutionPlanProcessorForStreamingQuerySuite extends StreamTest {
  import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

  val brokerProps: Map[String, Object] = Map[String, Object]()
  var testUtils: KafkaTestUtils = _

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    testHelperQueryListener.clear()
    spark.listenerManager.register(testHelperQueryListener)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    atlasClient = null
    spark.listenerManager.unregister(testHelperQueryListener)
    super.afterAll()
  }

  test("Kafka source(s) to kafka sink - micro-batch query") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val topicsToRead = Seq("sparkread1", "sparkread2", "sparkread3")
    val topicToWrite = "sparkwrite"
    val topics = topicsToRead :+ topicToWrite

    val brokerAddress = testUtils.brokerAddress

    topics.foreach(testUtils.createTopic(_, 10, overwrite = true))

    val tempDir = Files.createTempDirectory("spark-atlas-kafka-harvester")

    // remove temporary directory in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          Files.deleteIfExists(tempDir)
        }
      }, 10)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", topicsToRead.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    val query = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topicToWrite)
      .option("checkpointLocation", tempDir.toAbsolutePath.toString)
      .start()

    try {
      sendMessages(topicsToRead)
      waitForBatchCompleted(query, testHelperQueryListener)

      val queryDetail = testHelperQueryListener.queryDetails.head
      planProcessor.process(queryDetail)
      val entities = atlasClient.createdEntities

      assertEntitiesKafkaTopicType(topics, entities)
      assertEntitySparkProcessType(topicsToRead, topicToWrite, entities, queryDetail)
    } finally {
      query.stop()
    }
  }

  private def sendMessages(topicsToRead: Seq[String]): Unit = {
    topicsToRead.foreach { topic =>
      testUtils.sendMessages(topic, Array("1", "2", "3", "4", "5"))
    }
  }

  private def waitForBatchCompleted(query: StreamingQuery, listener: AtlasQueryExecutionListener)
  : Unit = {
    import org.scalatest.time.SpanSugar._
    eventually(timeout(10.seconds)) {
      query.processAllAvailable()
      assert(listener.queryDetails.nonEmpty)
    }
  }

  private def assertEntitiesKafkaTopicType(topics: Seq[String], entities: Seq[AtlasEntity])
    : Unit = {
    val kafkaTopicEntities = listAtlasEntitiesAsType(entities, KAFKA_TOPIC_STRING)
    assert(kafkaTopicEntities.size === topics.size)

    assert(kafkaTopicEntities.map(getStringAttribute(_, "name")).toSet === topics.toSet)
    assert(kafkaTopicEntities.map(getStringAttribute(_, "topic")).toSet === topics.toSet)
    assert(kafkaTopicEntities.map(getStringAttribute(_, "uri")).toSet === topics.toSet)
  }

  private def assertEntitySparkProcessType(topicsToRead: Seq[String], topicToWrite: String,
                                           entities: Seq[AtlasEntity], queryDetail: QueryDetail)
    : Unit = {
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    assert(!inputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))
    assert(!outputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))

    assert(inputs.map(getStringAttribute(_, "name")).toSet === topicsToRead.toSet)
    assert(outputs.map(getStringAttribute(_, "name")).toSet === Seq(topicToWrite).toSet)

    // verify others
    val expectedMap = Map(
      "executionId" -> queryDetail.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(queryDetail.qe),
      "executionTime" -> queryDetail.executionTime.toString,
      "details" -> queryDetail.qe.toString()
    )

    expectedMap.foreach { case (key, value) =>
      assert(processEntity.getAttribute(key) === value)
    }
  }
}