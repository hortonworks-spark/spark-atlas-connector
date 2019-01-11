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
import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor, KafkaTopicEntityValidator}
import com.hortonworks.spark.atlas.types.external.KAFKA_TOPIC_STRING
import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.streaming.KafkaTopicInformation

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery}

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JArray, JInt, JObject}

class SparkExecutionPlanProcessorForStreamingQuerySuite
  extends StreamTest
  with KafkaTopicEntityValidator {
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
    // NOTE: We can't test custom Atlas cluster here, because it requires Spark 2.3 to be patched.
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val topicsToRead1 = Seq("sparkread1", "sparkread2", "sparkread3")
    val topicsToRead2 = Seq("sparkread4", "sparkread5")
    val topicsToRead3 = Seq("sparkread6", "sparkread7")
    val topicToWrite = "sparkwrite"

    val topicsToRead = topicsToRead1 ++ topicsToRead2 ++ topicsToRead3
    val topics = topicsToRead ++ Seq(topicToWrite)

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

    // NOTE: custom atlas cluster name can't be tested here since it requires custom patch on Spark

    // test for 'subscribePattern'
    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribePattern", "sparkread[1-3]")
      .option("startingOffsets", "earliest")
      .load()

    // test for 'subscribe'
    val df2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", topicsToRead2.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    // test for 'assign'
    val jsonToAssignTopicToRead3 = {
      val r = JObject.apply {
        topicsToRead3.map {
          (_, JArray((0 until 10).map(JInt(_)).toList))
        }.toList
      }
      compact(render(r))
    }

    val df3 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("assign", jsonToAssignTopicToRead3)
      .option("startingOffsets", "earliest")
      .load()

    val query = df1.union(df2).union(df3).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topicToWrite)
      .option("checkpointLocation", tempDir.toAbsolutePath.toString)
      .start()

    try {
      sendMessages(topicsToRead1)
      sendMessages(topicsToRead2)
      sendMessages(topicsToRead3)
      waitForBatchCompleted(query, testHelperQueryListener)

      import org.scalatest.time.SpanSugar._
      var queryDetails: Seq[QueryDetail] = null
      var entitySet: Set[AtlasEntity] = null
      eventually(timeout(30.seconds)) {
        queryDetails = testHelperQueryListener.queryDetails
        queryDetails.foreach(planProcessor.process)

        val createdEntities = atlasClient.createdEntities
        logInfo(s"Count of created entities (with duplication): ${createdEntities.size}")
        entitySet = getUniqueEntities(createdEntities)
        logInfo(s"Count of created entities after deduplication: ${entitySet.size}")

        // spark_process, topic to write, topics to read group 1 and 2 and 3
        assert(entitySet.size == topicsToRead1.size + topicsToRead2.size + topicsToRead3.size + 2)
      }

      assertEntitiesKafkaTopicType(topics, entitySet)
      assertEntitySparkProcessType(topicsToRead, topicToWrite, entitySet, queryDetails)
    } finally {
      query.stop()
    }
  }

  private def sendMessages(topicsToRead: Seq[String]): Unit = {
    topicsToRead.foreach { topic =>
      testUtils.sendMessages(topic, (1 to 1000).map(_.toString).toArray)
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

  private def assertEntitySparkProcessType(
      topicsToRead: Seq[String],
      topicToWrite: String,
      entities: Set[AtlasEntity],
      queryDetails: Seq[QueryDetail]): Unit = {
    val processEntity = getOnlyOneEntity(entities.toSeq, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    assert(!inputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))
    assert(!outputs.exists(_.getTypeName != KAFKA_TOPIC_STRING))

    assert(inputs.map(getStringAttribute(_, "name")).toSet === topicsToRead.toSet)
    assert(outputs.map(getStringAttribute(_, "name")).toSet === Seq(topicToWrite).toSet)

    // unfortunately each batch recognizes topics which topics are having records to process
    // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
    assert(inputs.map(getStringAttribute(_, "qualifiedName")).toSet.subsetOf(
      topicsToRead.map(_ + "@primary").toSet))

    assert(outputs.map(getStringAttribute(_, "qualifiedName")).toSet ===
      Seq(topicToWrite).map(_ + "@primary").toSet)

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
}
