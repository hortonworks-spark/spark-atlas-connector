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
import java.nio.file.{Files, Path}
import java.util.Locale

import com.hortonworks.spark.atlas.sql.testhelper.{AtlasQueryExecutionListener, CreateEntitiesTrackingAtlasClient, DirectProcessSparkExecutionPlanProcessor, KafkaTopicEntityValidator}
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.AtlasClientConf
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.StreamTest
import org.json4s.JsonAST.{JArray, JInt, JObject}
import org.json4s.jackson.JsonMethods.{compact, render}

class SparkExecutionPlanProcessorForBatchQuerySuite
  extends StreamTest
  with KafkaTopicEntityValidator {
  import com.hortonworks.spark.atlas.sql.testhelper.AtlasEntityReadHelper._

  val brokerProps: Map[String, Object] = Map[String, Object]()
  var kafkaTestUtils: KafkaTestUtils = _

  val atlasClientConf: AtlasClientConf = new AtlasClientConf()
    .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
  var atlasClient: CreateEntitiesTrackingAtlasClient = _
  val testHelperQueryListener = new AtlasQueryExecutionListener()

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaTestUtils = new KafkaTestUtils(brokerProps)
    kafkaTestUtils.setup()
    atlasClient = new CreateEntitiesTrackingAtlasClient()
    testHelperQueryListener.clear()
    spark.listenerManager.register(testHelperQueryListener)
  }

  override def afterAll(): Unit = {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
    atlasClient = null
    spark.listenerManager.unregister(testHelperQueryListener)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    atlasClient.clearEntities()
  }

  test("Read csv file and save as table") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val csvContent = Seq("a,1", "b,2", "c,3", "d,4").mkString("\n")
    val tempFile: Path = writeCSVtextToTempFile(csvContent)

    val rand = new scala.util.Random()
    val outputTableName = "app_details_" + rand.nextInt(1000000000)

    val df = spark.read.csv(tempFile.toAbsolutePath.toString)
    df.write.saveAsTable(outputTableName)

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)
    val entities = atlasClient.createdEntities

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)
    assertTableEntity(tableEntity, outputTableName)

    // we're expecting one file system entities: input file
    val fsEntities = listAtlasEntitiesAsType(entities, external.FS_PATH_TYPE_STRING)
    assert(fsEntities.size === 1)

    val databaseEntity = getOnlyOneEntity(entities, metadata.DB_TYPE_STRING)
    assertDatabaseEntity(databaseEntity, tableEntity, outputTableName)

    // input file
    // this code asserts on runtime that one of fs entity matches against source path
    val sourcePath = tempFile.toAbsolutePath.toString
    val inputFsEntity = fsEntities.find { p =>
      getStringAttribute(p, "name").toLowerCase(Locale.ROOT) == sourcePath.toLowerCase(Locale.ROOT)
    }.get
    assertInputFsEntity(inputFsEntity, sourcePath)

    // storage description
    val storageEntity = getOnlyOneEntity(entities, metadata.STORAGEDESC_TYPE_STRING)
    assertStorageDefinitionEntity(storageEntity, tableEntity)

    val locationString = getStringAttribute(storageEntity, "location")
    assert(locationString != null && locationString.nonEmpty)
    assert(locationString.contains("spark-warehouse"))
    assert(locationString.contains(outputTableName))

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    val input = getOnlyOneEntity(inputs, external.FS_PATH_TYPE_STRING)
    val output = getOnlyOneEntity(outputs, metadata.TABLE_TYPE_STRING)

    // input/output in 'spark_process' should be same as outer entities
    assert(input === inputFsEntity)
    assert(output === tableEntity)

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

  test("Create external table against JSON files") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val tempDirPath = writeJsonFilesToTempDirectory()
    val tempDirPathStr = tempDirPath.toFile.getAbsolutePath

    val rand = new scala.util.Random()
    val outputTableName = "test_json_" + rand.nextInt(1000000000)
    spark.sql(s"CREATE TABLE $outputTableName USING json location '$tempDirPathStr'")

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)

    val entities = atlasClient.createdEntities

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)
    assertTableEntity(tableEntity, outputTableName)

    // database
    val databaseEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.DB_TYPE_STRING)
    assertDatabaseEntity(databaseEntity, tableEntity, outputTableName)

    // storage description
    val storageEntity = getOnlyOneEntity(entities, metadata.STORAGEDESC_TYPE_STRING)
    assertStorageDefinitionEntity(storageEntity, tableEntity)

    val databaseLocationString = getStringAttribute(databaseEntity, "location")
    assert(databaseLocationString != null && databaseLocationString.nonEmpty)
    assert(databaseLocationString.contains("spark-warehouse"))
  }

  test("Save Spark table to Kafka via df.save()") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val rand = new scala.util.Random()
    val inputTableName = "test_spark_table_" + rand.nextInt(1000000000)
    val outputTopicName = "test_spark_topic_" + rand.nextInt(1000000000)

    val csvContent = Seq("a,1", "b,2", "c,3", "d,4").mkString("\n")
    val tempFile: Path = writeCSVtextToTempFile(csvContent)

    val df = spark.read.csv(tempFile.toAbsolutePath.toString)
    df.write.saveAsTable(inputTableName)

    // we don't want to check above queries, so reset the entities in listener
    testHelperQueryListener.clear()

    val customClusterName = "customCluster"

    spark
      .sql(s"select * from $inputTableName")
      .selectExpr("cast(_c0 as String) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("topic", outputTopicName)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, customClusterName)
      .save()

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)

    val entities = atlasClient.createdEntities

    // We already have validations for table-relevant entities in other UTs,
    // so minimize validation here.

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)
    assertTableEntity(tableEntity, inputTableName)

    // kafka topic
    val outputKafkaEntity = getOnlyOneEntity(entities, external.KAFKA_TOPIC_STRING)
    val expectedTopics = Seq(
      KafkaTopicInformation(outputTopicName, Some(customClusterName))
    )
    assertEntitiesKafkaTopicType(expectedTopics, entities.toSet)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    val input = getOnlyOneEntity(inputs, metadata.TABLE_TYPE_STRING)
    val output = getOnlyOneEntity(outputs, external.KAFKA_TOPIC_STRING)

    // input/output in 'spark_process' should be same as outer entities
    assert(input === tableEntity)
    assert(output === outputKafkaEntity)

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

  test("Read Kafka topics with various options of subscription " +
    "and save to Spark table via df.saveAsTable()") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val rand = new scala.util.Random()
    val topicsToRead1 = Seq("sparkread1", "sparkread2", "sparkread3")
    val topicsToRead2 = Seq("sparkread4", "sparkread5")
    val topicsToRead3 = Seq("sparkread6", "sparkread7")
    val topics = topicsToRead1 ++ topicsToRead2 ++ topicsToRead3

    val outputTableName = "test_spark_table_" + rand.nextInt(1000000000)

    topics.toSet[String].foreach { ti =>
      kafkaTestUtils.createTopic(ti, 10, overwrite = true)
    }

    sendMessages(topics)

    // test for 'subscribePattern'
    val df1 = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("subscribePattern", "sparkread[1-3]")
      .option("startingOffsets", "earliest")
      .load()

    // test for 'subscribe'
    val df2 = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("subscribe", topicsToRead2.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    // test for 'assign'
    val jsonToAssignTopicToRead3 = {
      val r = JObject.apply {
        topicsToRead3.map {
          (_, JArray(List(JInt(0), JInt(1))))
        }.toList
      }
      compact(render(r))
    }

    val df3 = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("assign", jsonToAssignTopicToRead3)
      .option("startingOffsets", "earliest")
      .load()

    df1.union(df2).union(df3).write.mode("append").saveAsTable(outputTableName)

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)

    val entities = atlasClient.createdEntities

    // kafka topic

    // actual topics in 'subscribePattern' cannot be retrieved - it's a limitation
    val inputKafkaEntities = listAtlasEntitiesAsType(entities, external.KAFKA_TOPIC_STRING)
    val expectedTopics = (topicsToRead2 ++ topicsToRead3).map(KafkaTopicInformation(_, None))
    assertEntitiesKafkaTopicType(expectedTopics, entities.toSet)

    // We already have validations for table-relevant entities in other UTs,
    // so minimize validation here.

    val tableEntity: AtlasEntity = getOnlyOneEntity(entities, metadata.TABLE_TYPE_STRING)
    assertTableEntity(tableEntity, outputTableName)

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    val output = getOnlyOneEntity(outputs, metadata.TABLE_TYPE_STRING)

    // input/output in 'spark_process' should be same as outer entities
    assert(inputs.toSet === inputKafkaEntities.toSet)
    assert(output === tableEntity)

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

  test("Read Kafka topics and save to Kafka via df.save()") {
    val planProcessor = new DirectProcessSparkExecutionPlanProcessor(atlasClient, atlasClientConf)

    val topicsToRead = Seq("sparkread1", "sparkread2", "sparkread3")
    val topicToWrite = "sparkwrite"
    val topics = topicsToRead ++ Seq(topicToWrite)

    topics.toSet[String].foreach { ti =>
      kafkaTestUtils.createTopic(ti, 10, overwrite = true)
    }

    sendMessages(topicsToRead)

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("subscribe", topicsToRead.mkString(","))
      .option("startingOffsets", "earliest")
      .load()

    val customClusterName = "customCluster"
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("topic", topicToWrite)
      .option("kafka." + AtlasClientConf.CLUSTER_NAME.key, customClusterName)
      .save()

    val queryDetail = testHelperQueryListener.queryDetails.last
    planProcessor.process(queryDetail)

    val entities = atlasClient.createdEntities

    // kafka topic
    val kafkaEntities = listAtlasEntitiesAsType(entities, external.KAFKA_TOPIC_STRING)
    assert(kafkaEntities.size === topicsToRead.length + 1)

    val inputKafkaEntities = kafkaEntities.filter { entity =>
      topicsToRead.contains(entity.getAttribute("name"))
    }
    val expectedTopics = topicsToRead.map(KafkaTopicInformation(_, None))
    assertEntitiesKafkaTopicType(expectedTopics, inputKafkaEntities.toSet)

    val outputEntities = kafkaEntities.filter { entity =>
      entity.getAttribute("name") == topicToWrite
    }

    // check for 'spark_process'
    val processEntity = getOnlyOneEntity(entities, metadata.PROCESS_TYPE_STRING)

    val inputs = getSeqAtlasEntityAttribute(processEntity, "inputs")
    val outputs = getSeqAtlasEntityAttribute(processEntity, "outputs")

    // input/output in 'spark_process' should be same as outer entities
    assert(inputs.toSet === inputKafkaEntities.toSet)
    assert(outputs === outputEntities)

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

  private def writeCSVtextToTempFile(csvContent: String) = {
    val tempFile = Files.createTempFile("spark-atlas-connector-csv-temp", ".csv")

    // remove temporary file in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          Files.deleteIfExists(tempFile)
        }
      }, 10)

    val bw = new BufferedWriter(new FileWriter(tempFile.toFile))
    bw.write(csvContent)
    bw.close()
    tempFile
  }

  private def writeJsonFilesToTempDirectory(): Path = {
    val tempParentDir = Files.createTempDirectory("spark-atlas-connector-json-temp")
    val tempDir = new File(tempParentDir.toFile, "json")
    val tempDirPath = tempDir.getAbsolutePath

    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          FileUtils.deleteQuietly(tempParentDir.toFile)
        }
      }, 10)

    spark.range(10).write.json(tempDirPath)

    tempDir.toPath
  }

  private def sendMessages(topicsToRead: Seq[String]): Unit = {
    topicsToRead.foreach { topic =>
      kafkaTestUtils.sendMessages(topic, Array("1", "2", "3", "4", "5"))
    }
  }

  private def assertTableEntity(tableEntity: AtlasEntity, tableName: String): Unit = {
    // only assert qualifiedName and skip assertion on database, and attributes on database
    // they should be covered in other UT
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    assert(tableQualifiedName.endsWith(tableName))
  }

  private def assertDatabaseEntity(
      databaseEntity: AtlasEntity,
      tableEntity: AtlasEntity,
      tableName: String)
    : Unit = {
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    // remove table name + '.' prior to table name
    val databaseQualifiedName = tableQualifiedName.substring(0,
      tableQualifiedName.indexOf(tableName) - 1)

    assert(getStringAttribute(databaseEntity, "qualifiedName") === databaseQualifiedName)

    // database entity in table entity should be same as outer database entity
    val databaseEntityInTable = getAtlasEntityAttribute(tableEntity, "db")
    assert(databaseEntity === databaseEntityInTable)

    val databaseLocationString = getStringAttribute(databaseEntity, "location")
    assert(databaseLocationString != null && databaseLocationString.nonEmpty)
    assert(databaseLocationString.contains("spark-warehouse"))
  }

  private def assertInputFsEntity(fsEntity: AtlasEntity, sourcePath: String): Unit = {
    assertPathsEquals(getStringAttribute(fsEntity, "name"), sourcePath)
    assertPathsEquals(getStringAttribute(fsEntity, "path"), sourcePath)
    assertPathsEquals(getStringAttribute(fsEntity, "qualifiedName"),
      "file://" + sourcePath)
  }

  private def assertStorageDefinitionEntity(sdEntity: AtlasEntity, tableEntity: AtlasEntity)
    : Unit = {
    val tableQualifiedName = getStringAttribute(tableEntity, "qualifiedName")
    val storageQualifiedName = tableQualifiedName + ".storageFormat"
    assert(getStringAttribute(sdEntity, "qualifiedName") === storageQualifiedName)

    val storageEntityInTableAttribute = getAtlasEntityAttribute(tableEntity, "sd")
    assert(sdEntity === storageEntityInTableAttribute)
  }

  private def assertPathsEquals(path1: String, path2: String): Unit = {
    // we are comparing paths with lower case, since we may want to run the test
    // from case-insensitive filesystem
    assert(path1.toLowerCase(Locale.ROOT) === path2.toLowerCase(Locale.ROOT))
  }
}
