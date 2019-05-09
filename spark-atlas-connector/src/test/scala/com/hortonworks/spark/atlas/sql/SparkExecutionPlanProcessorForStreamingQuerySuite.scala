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

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files

import com.hortonworks.spark.atlas.sql.testhelper._
import com.hortonworks.spark.atlas.types.external.KAFKA_TOPIC_STRING
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.{AtlasClientConf, AtlasUtils, TestUtils}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JArray, JInt, JObject}

class SparkExecutionPlanProcessorForStreamingQuerySuite
  extends StreamTest
  with KafkaTopicEntityValidator
  with FsEntityValidator {
  import com.hortonworks.spark.atlas.AtlasEntityReadHelper._

  val brokerProps: Map[String, Object] = Map[String, Object]()
  var testUtils: KafkaTestUtils = _

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()
  val testHelperStreamingQueryListener = new AtlasStreamingQueryProgressListener()

  var planProcessor: DirectProcessSparkExecutionPlanProcessor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    testHelperQueryListener.clear()
    spark.listenerManager.register(testHelperQueryListener)
    spark.streams.addListener(testHelperStreamingQueryListener)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    atlasClient = null
    spark.listenerManager.unregister(testHelperQueryListener)
    spark.streams.removeListener(testHelperStreamingQueryListener)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    atlasClient.clearEntities()
    testHelperQueryListener.clear()
    testHelperStreamingQueryListener.clear()

    planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)
  }

  test("file source(s) to file sink - micro-batch query") {
    val (srcDir, destDir, checkpointDir) = createTempDirectories

    val df = spark
      .readStream
      .text(srcDir.getAbsolutePath)

    val query = toFileSink(df, "text", destDir.getAbsolutePath, checkpointDir.getAbsolutePath)

    try {
      writeTestTextFiles(srcDir)

      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, path to read, path to write
      assert(entitySet.size == 13)

      // input path and output path (11 + 1)
      val fsEntities = listAtlasEntitiesAsType(entitySet.toSeq, external.FS_PATH_TYPE_STRING)
      assert(fsEntities.size === 12)

      val inputFsEntities = findFsEntities(fsEntities, srcDir)
      assert(inputFsEntities.length === 11)

      val outputFsEntities = findFsEntities(fsEntities, destDir)
      assert(outputFsEntities.length === 1)

      // check for 'spark_process'
      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        val input = listAtlasEntitiesAsType(inputs, external.FS_PATH_TYPE_STRING)
        TestUtils.assertSubsetOf(inputFsEntities.toSet, input.toSet)
      }, outputs => {
        val output = getOnlyOneEntity(outputs, external.FS_PATH_TYPE_STRING)
        assert(output === outputFsEntities.head)
      })
    } finally {
      query.stop()
    }
  }

  test("Kafka source(s) to kafka sink - micro-batch query") {
    val topicPrefix = "sparkread0"
    val brokerAddress = testUtils.brokerAddress

    val customClusterName1 = "customCluster1"
    val (df1, topicsToRead1WithClusterInfo) =
      kafkaDfSubscribePattern(topicPrefix, customClusterName1, brokerAddress)

    val customClusterName2 = "customCluster2"
    val (df2, topicsToRead2WithClusterInfo) =
      kafkaDfSubscribe(topicPrefix, customClusterName2, brokerAddress)

    val customClusterName3 = "customCluster3"
    val (df3, topicsToRead3WithClusterInfo) =
      kafkaDfAssign(topicPrefix, customClusterName3, brokerAddress)

    val topicsToRead = topicsToRead1WithClusterInfo.map(_.topicName) ++
      topicsToRead2WithClusterInfo.map(_.topicName) ++
      topicsToRead3WithClusterInfo.map(_.topicName)

    val topicToWrite = "sparkwrite"
    val topics = topicsToRead ++ Seq(topicToWrite)

    topics.toSet[String].foreach { ti =>
      testUtils.createTopic(ti, 10, overwrite = true)
    }

    val (_, _, checkpointDir) = createTempDirectories

    val df = df1.union(df2).union(df3)

    val customClusterNameForWriter = "customCluster4"
    val query = toKafkaSink(df, brokerAddress, Some(customClusterNameForWriter),
      topicToWrite, checkpointDir.getAbsolutePath.toString)

    try {
      sendMessages(topicsToRead)
      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, topic to write, topics to read group 1, 2 and 3
      assert(entitySet.size == topicsToRead.size + 2)

      val topicToWriteWithClusterInfo = KafkaTopicInformation(topicToWrite,
        Some(customClusterNameForWriter))

      val topicsToReadWithClusterInfo = topicsToRead1WithClusterInfo ++
        topicsToRead2WithClusterInfo ++ topicsToRead3WithClusterInfo
      val topicsWithClusterInfo = topicsToReadWithClusterInfo ++ Seq(topicToWriteWithClusterInfo)

      assertEntitiesKafkaTopicType(topicsWithClusterInfo, entitySet)

      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        // unfortunately each batch recognizes topics which topics are having records to process
        // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
        assertEntitiesAreSubsetOfTopics(topicsToReadWithClusterInfo, inputs)
      }, outputs => {
        assertEntitiesAreSubsetOfTopics(Seq(topicToWriteWithClusterInfo), outputs)
      })
    } finally {
      query.stop()
    }
  }

  test("kafka source to file sink - micro-batch query") {
    val topicPrefix = "sparkread1"
    val brokerAddress = testUtils.brokerAddress

    val customClusterName1 = "customCluster1"
    val (df1, topicsToRead1WithClusterInfo) =
      kafkaDfSubscribePattern(topicPrefix, customClusterName1, brokerAddress)

    val customClusterName2 = "customCluster2"
    val (df2, topicsToRead2WithClusterInfo) =
      kafkaDfSubscribe(topicPrefix, customClusterName2, brokerAddress)

    val customClusterName3 = "customCluster3"
    val (df3, topicsToRead3WithClusterInfo) =
      kafkaDfAssign(topicPrefix, customClusterName3, brokerAddress)

    val topicsToRead = topicsToRead1WithClusterInfo.map(_.topicName) ++
      topicsToRead2WithClusterInfo.map(_.topicName) ++
      topicsToRead3WithClusterInfo.map(_.topicName)

    val topicToWrite = "sparkwrite1"
    val topics = topicsToRead ++ Seq(topicToWrite)

    topics.toSet[String].foreach { ti =>
      testUtils.createTopic(ti, 10, overwrite = true)
    }

    val (_, destDir, checkpointDir) = createTempDirectories

    val df = df1.union(df2).union(df3)

    val query = toFileSink(df, "json", destDir.getAbsolutePath, checkpointDir.getAbsolutePath)

    try {
      sendMessages(topicsToRead)
      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, file sink, topics to read group 1, 2 and 3
      assert(entitySet.size == topicsToRead.size + 2)

      val topicsToReadWithClusterInfo = topicsToRead1WithClusterInfo ++
        topicsToRead2WithClusterInfo ++ topicsToRead3WithClusterInfo

      assertEntitiesKafkaTopicType(topicsToReadWithClusterInfo, entitySet)

      val fsEntities = listAtlasEntitiesAsType(entitySet.toSeq, external.FS_PATH_TYPE_STRING)
      assert(fsEntities.size === 1)

      val outputFsEntities = findFsEntities(fsEntities, destDir)
      assert(outputFsEntities == fsEntities)

      // check for 'spark_process'
      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        // unfortunately each batch recognizes topics which topics are having records to process
        // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
        assertEntitiesAreSubsetOfTopics(topicsToReadWithClusterInfo, inputs)
      }, outputs => {
        assert(outputs.toSet === outputFsEntities.toSet)
      })
    } finally {
      query.stop()
    }
  }

  test("file source to kafka sink - micro-batch query") {
    val topicToWrite = "sparkwrite"

    val brokerAddress = testUtils.brokerAddress

    val (srcDir, destDir, checkpointDir) = createTempDirectories

    val df = spark
      .readStream
      .text(srcDir.getAbsolutePath)

    val customClusterNameForWriter = "customCluster4"
    val query = toKafkaSink(df, brokerAddress, Some(customClusterNameForWriter),
      topicToWrite, checkpointDir.getAbsolutePath)

    try {
      writeTestTextFiles(srcDir)

      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, files to read (11), topic to write
      assert(entitySet.size == 13)

      // input path and output path
      val fsEntities = listAtlasEntitiesAsType(entitySet.toSeq, external.FS_PATH_TYPE_STRING)
      assert(fsEntities.size === 11)

      val inputFsEntities = findFsEntities(fsEntities, srcDir)
      assert(inputFsEntities.length === 11)

      val topicToWriteWithClusterInfo = KafkaTopicInformation(topicToWrite,
        Some(customClusterNameForWriter))

      assertEntitiesKafkaTopicType(Seq(topicToWriteWithClusterInfo), entitySet)
      val kafkaEntities = listAtlasEntitiesAsType(entitySet.toSeq, KAFKA_TOPIC_STRING)

      // check for 'spark_process'
      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        // unfortunately each batch recognizes topics which topics are having records to process
        // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
        TestUtils.assertSubsetOf(
          inputs.map(getStringAttribute(_, "qualifiedName")).toSet,
          inputFsEntities.map(getStringAttribute(_, "qualifiedName")).toSet)
      }, outputs => {
        assert(outputs.toSet === kafkaEntities.toSet)
      })
    } finally {
      query.stop()
    }
  }
  test("file & kafka source to file sink - micro-batch query") {
    val (srcDir, destDir, checkpointDir) = createTempDirectories

    val df1 = spark
      .readStream
      .text(srcDir.getAbsolutePath)

    val topicPrefix = "sparkread1"
    val brokerAddress = testUtils.brokerAddress

    val customClusterName1 = "customCluster1"
    val (df2, topicsToRead1WithClusterInfo) =
      kafkaDfSubscribePattern(topicPrefix, customClusterName1, brokerAddress)

    val customClusterName2 = "customCluster2"
    val (df3, topicsToRead2WithClusterInfo) =
      kafkaDfSubscribe(topicPrefix, customClusterName2, brokerAddress)

    val customClusterName3 = "customCluster3"
    val (df4, topicsToRead3WithClusterInfo) =
      kafkaDfAssign(topicPrefix, customClusterName3, brokerAddress)

    val topicsToRead = topicsToRead1WithClusterInfo.map(_.topicName) ++
      topicsToRead2WithClusterInfo.map(_.topicName) ++
      topicsToRead3WithClusterInfo.map(_.topicName)

    topicsToRead.toSet[String].foreach { ti =>
      testUtils.createTopic(ti, 10, overwrite = true)
    }

    val df = df1
      .union(df2.selectExpr("CAST(value AS STRING) AS value"))
      .union(df3.selectExpr("CAST(value AS STRING) AS value"))
      .union(df4.selectExpr("CAST(value AS STRING) AS value"))

    val query = toFileSink(df, "json", destDir.getAbsolutePath, checkpointDir.getAbsolutePath)

    try {
      writeTestTextFiles(srcDir)
      sendMessages(topicsToRead)
      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, file sink, topics to read group 1, 2 and 3, path to read
      assert(entitySet.size == topicsToRead.size + 13)

      val topicsToReadWithClusterInfo = topicsToRead1WithClusterInfo ++
        topicsToRead2WithClusterInfo ++ topicsToRead3WithClusterInfo

      assertEntitiesKafkaTopicType(topicsToReadWithClusterInfo, entitySet)

      val fsEntities = listAtlasEntitiesAsType(entitySet.toSeq, external.FS_PATH_TYPE_STRING)
      assert(fsEntities.size === 12)

      val inputFsEntities = findFsEntities(fsEntities, srcDir)
      assert(inputFsEntities.length === 11)

      val outputFsEntities = findFsEntities(fsEntities, destDir)
      assert(outputFsEntities.length === 1)

      // check for 'spark_process'
      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        val kafkaInputs = listAtlasEntitiesAsType(inputs, external.KAFKA_TOPIC_STRING)
        val fsInputs = listAtlasEntitiesAsType(inputs, external.FS_PATH_TYPE_STRING)
          // unfortunately each batch recognizes topics which topics are having records to process
        // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
        assertEntitiesAreSubsetOfTopics(topicsToReadWithClusterInfo, kafkaInputs)
        assertEntitiesFsType(Map(srcDir -> 11), fsInputs.toSet)
      }, outputs => {
        assert(outputs.toSet === outputFsEntities.toSet)
      })
    } finally {
      query.stop()
    }
  }

  test("file & kafka source to kafka sink - micro-batch query") {
    val (srcDir, destDir, checkpointDir) = createTempDirectories

    val df1 = spark
      .readStream
      .text(srcDir.getAbsolutePath)

    val topicPrefix = "sparkread1"
    val brokerAddress = testUtils.brokerAddress

    val customClusterName1 = "customCluster1"
    val (df2, topicsToRead1WithClusterInfo) =
      kafkaDfSubscribePattern(topicPrefix, customClusterName1, brokerAddress)

    val customClusterName2 = "customCluster2"
    val (df3, topicsToRead2WithClusterInfo) =
      kafkaDfSubscribe(topicPrefix, customClusterName2, brokerAddress)

    val customClusterName3 = "customCluster3"
    val (df4, topicsToRead3WithClusterInfo) =
      kafkaDfAssign(topicPrefix, customClusterName3, brokerAddress)

    val topicsToRead = topicsToRead1WithClusterInfo.map(_.topicName) ++
      topicsToRead2WithClusterInfo.map(_.topicName) ++
      topicsToRead3WithClusterInfo.map(_.topicName)

    val topicToWrite = "sparkwrite1"
    val topics = topicsToRead ++ Seq(topicToWrite)

    topics.toSet[String].foreach { ti =>
      testUtils.createTopic(ti, 10, overwrite = true)
    }

    val df = df1
      .union(df2.selectExpr("CAST(value AS STRING) AS value"))
      .union(df3.selectExpr("CAST(value AS STRING) AS value"))
      .union(df4.selectExpr("CAST(value AS STRING) AS value"))

    val customClusterNameForWriter = "customCluster4"
    val query = toKafkaSink(df, brokerAddress, Some(customClusterNameForWriter),
      topicToWrite, checkpointDir.getAbsolutePath.toString)

    try {
      writeTestTextFiles(srcDir)
      sendMessages(topicsToRead)
      waitForBatchCompleted(query)

      val (queryDetails, entitySet) = processQueryDetails
      // spark_process, write topic, topics to read group 1, 2 and 3, path to read
      assert(entitySet.size == topicsToRead.size + 13)

      val topicsToReadWithClusterInfo = topicsToRead1WithClusterInfo ++
        topicsToRead2WithClusterInfo ++ topicsToRead3WithClusterInfo
      val topicToWriteWithClusterInfo = KafkaTopicInformation(topicToWrite,
        Some(customClusterNameForWriter))

      val topicsWithClusterInfo = topicsToReadWithClusterInfo ++ Seq(topicToWriteWithClusterInfo)

      assertEntitiesKafkaTopicType(topicsWithClusterInfo, entitySet)

      val fsEntities = listAtlasEntitiesAsType(entitySet.toSeq, external.FS_PATH_TYPE_STRING)
      assert(fsEntities.size === 11)

      val inputFsEntities = findFsEntities(fsEntities, srcDir)
      assert(inputFsEntities === fsEntities)

      // check for 'spark_process'
      assertEntitySparkProcessType(entitySet, queryDetails, inputs => {
        val kafkaInputs = listAtlasEntitiesAsType(inputs, external.KAFKA_TOPIC_STRING)
        val fsInputs = listAtlasEntitiesAsType(inputs, external.FS_PATH_TYPE_STRING)
        // unfortunately each batch recognizes topics which topics are having records to process
        // so there's no guarantee that all topics are recognized as 'inputs' for 'spark_process'
        assertEntitiesAreSubsetOfTopics(topicsToReadWithClusterInfo, kafkaInputs)
        assertEntitiesFsType(Map(srcDir -> 11), fsInputs.toSet)
      }, outputs => {
        assertEntitiesAreSubsetOfTopics(Seq(topicToWriteWithClusterInfo), outputs)
      })
    } finally {
      query.stop()
    }
  }

  private def kafkaDfSubscribePattern(
      topicNamePrefix: String,
      customClusterName: String,
      brokerAddress: String): (Dataset[Row], Seq[KafkaTopicInformation]) = {
    val topics = (1 to 3).map(idx => topicNamePrefix + idx)
    val topicsWithClusterInfo = topics.map { tp =>
      KafkaTopicInformation(tp, Some(customClusterName))
    }

    // test for 'subscribePattern'
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, customClusterName)
      .option("subscribePattern", topicNamePrefix + "[1-3]")
      .option("startingOffsets", "earliest")
      .load()

    (df, topicsWithClusterInfo)
  }

  private def kafkaDfSubscribe(
      topicNamePrefix: String,
      customClusterName: String,
      brokerAddress: String): (Dataset[Row], Seq[KafkaTopicInformation]) = {
    val topics = (4 to 5).map(idx => topicNamePrefix + idx)
    val topicsWithClusterInfo = topics.map { tp =>
      KafkaTopicInformation(tp, Some(customClusterName))
    }

    // test for 'subscribe'
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, customClusterName)
      .option("subscribe", topics.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    (df, topicsWithClusterInfo)
  }

  private def kafkaDfAssign(
      topicNamePrefix: String,
      customClusterName: String,
      brokerAddress: String): (Dataset[Row], Seq[KafkaTopicInformation]) = {
    val topics = (6 to 7).map(idx => topicNamePrefix + idx)
    val topicsWithClusterInfo = topics.map { tp =>
      KafkaTopicInformation(tp, Some(customClusterName))
    }

    // test for 'assign'
    val jsonToAssignTopicToRead = {
      val r = JObject.apply {
        topics.map {
          (_, JArray((0 until 10).map(JInt(_)).toList))
        }.toList
      }
      compact(render(r))
    }

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, customClusterName)
      .option("assign", jsonToAssignTopicToRead)
      .option("startingOffsets", "earliest")
      .load()

    (df, topicsWithClusterInfo)
  }

  private def createTempDirectories: (File, File, File) = {
    val tempDir = Files.createTempDirectory("spark-atlas-streaming-files-temp")

    val srcDir = new File(tempDir.toFile, "src")
    val destDir = new File(tempDir.toFile, "dest")
    val checkpointDir = new File(tempDir.toFile, "checkpoint")

    // remove temporary directory in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          Files.deleteIfExists(tempDir)
        }
      }, 10)

    Files.createDirectories(srcDir.toPath)

    (srcDir, destDir, checkpointDir)
  }

  private def writeTestTextFiles(srcDir: File) = {
    (0 to 10).foreach { idx =>
      val writer = new BufferedWriter(new FileWriter(new File(srcDir, s"file-$idx.txt")))
      writer.write("Hello Scala")
      writer.close()
    }
  }

  private def toKafkaSink(
      df: Dataset[Row],
      brokerAddress: String,
      customClusterName: Option[String],
      topic: String,
      checkpointPath: String): StreamingQuery = {
    val dsWriter = df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .option("checkpointLocation", checkpointPath)

    customClusterName match {
      case Some(clName) =>
        dsWriter.option("kafka." + AtlasClientConf.CLUSTER_NAME.key, clName).start()

      case None =>
        dsWriter.start()
    }
  }

  private def toFileSink(
      df: Dataset[Row],
      format: String,
      path: String,
      checkpointPath: String): StreamingQuery = {
    df.writeStream
      .format(format)
      .option("path", path)
      .option("checkpointLocation", checkpointPath)
      .start()
  }

  private def sendMessages(topicsToRead: Seq[String]): Unit = {
    topicsToRead.foreach { topic =>
      testUtils.sendMessages(topic, (1 to 1000).map(_.toString).toArray)
    }
  }

  private def waitForBatchCompleted(query: StreamingQuery): Unit = {
    import org.scalatest.time.SpanSugar._
    eventually(timeout(10.seconds)) {
      query.processAllAvailable()
      val queryDetails = testHelperQueryListener.queryDetails ++
        testHelperStreamingQueryListener.queryDetails
      assert(queryDetails.nonEmpty)
    }
  }

  private def assertEntitySparkProcessType(
      entities: Set[AtlasEntity],
      queryDetails: Seq[QueryDetail],
      fnAssertInputs: Seq[AtlasEntity] => Unit,
      fnAssertOutputs: Seq[AtlasEntity] => Unit): Unit = {
    val processEntity = getOnlyOneEntity(entities.toSeq, metadata.PROCESS_TYPE_STRING)

    val inputIds = getSeqAtlasObjectIdAttribute(processEntity, "inputs")
    val outputIds = getSeqAtlasObjectIdAttribute(processEntity, "outputs")

    val inputs = entities.filter { entity =>
      inputIds.contains(AtlasUtils.entityToReference(entity, useGuid = false))
    }.toSeq
    val outputs = entities.filter { entity =>
      outputIds.contains(AtlasUtils.entityToReference(entity, useGuid = false))
    }.toSeq

    assert(inputIds.size == inputs.size)
    assert(outputIds.size == outputs.size)

    fnAssertInputs(inputs)
    fnAssertOutputs(outputs)

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

  private def processQueryDetails: (Seq[QueryDetail], Set[AtlasEntity]) = {
    val queryDetails: Seq[QueryDetail] = testHelperQueryListener.queryDetails ++
      testHelperStreamingQueryListener.queryDetails
    queryDetails.foreach(planProcessor.process)

    val createdEntities = atlasClient.createdEntities
    logInfo(s"Count of created entities (with duplication): ${createdEntities.size}")
    val entitySet: Set[AtlasEntity] = getUniqueEntities(createdEntities)
    logInfo(s"Count of created entities after deduplication: ${entitySet.size}")
    (queryDetails, entitySet)
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
