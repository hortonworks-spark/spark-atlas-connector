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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.sources.{MemoryWriterFactory, MicroBatchWriter}
import org.apache.spark.sql.kafka010.{KafkaStreamWriter, KafkaTestUtils}
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, StreamingQuery}
import org.apache.spark.sql.types.{BinaryType, StructType}

class KafkaHarvesterSuite extends StreamTest {
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

  test("Extract Kafka topic from MicroBatchWriter") {
    val topic = Some("hello")

    val writer = new KafkaStreamWriter(topic, producerParams, kafkaWriteSchema)
    val microBatchWriter = new MicroBatchWriter(0L, writer)

    assert(KafkaHarvester.extractTopic(microBatchWriter) === topic)
  }

  test("No Kafka topic information in WriterFactory") {
    val writer = new FakeStreamWriter()
    val microBatchWriter = new MicroBatchWriter(0L, writer)

    assert(KafkaHarvester.extractTopic(microBatchWriter) === None)
  }

  private class FakeStreamWriter extends StreamWriter {
    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

    override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

    override def createWriterFactory(): DataWriterFactory[InternalRow] = {
      MemoryWriterFactory(OutputMode.Append(), kafkaWriteSchema)
    }
  }

}
