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

package com.hortonworks.spark.atlas

import java.util.UUID

import com.hortonworks.spark.atlas.sql.KafkaTopicInformation
import com.hortonworks.spark.atlas.sql.testhelper.CreateEntitiesTrackingAtlasClient
import com.hortonworks.spark.atlas.types.{external, internal}
import org.apache.atlas.model.instance.AtlasEntity
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class AtlasEntityCreationRequestHelperSuite
  extends FunSuite
  with WithHiveSupport
  with BeforeAndAfterEach {

  private val client = new CreateEntitiesTrackingAtlasClient
  private var sut: AtlasEntityCreationRequestHelper = _

  override protected def beforeEach(): Unit = {
    client.clearEntities()
    sut = new AtlasEntityCreationRequestHelper(client)
  }

  test("SAC-253 partial sources presented in streaming query") {
    val cluster = "cl1"
    val queryId = UUID.randomUUID()

    val topic1 = KafkaTopicInformation("topic1")
    val topic2 = KafkaTopicInformation("topic2")
    val topic3 = KafkaTopicInformation("topic3")
    val topicSink = KafkaTopicInformation("topicSink")

    val source1 = external.kafkaToEntity(cluster, topic1)
    val source2 = external.kafkaToEntity(cluster, topic2)
    val source3 = external.kafkaToEntity(cluster, topic3)
    val sink = external.kafkaToEntity(cluster, topicSink)

    // source1
    validateInputsOutputs(queryId, Seq(source1), Seq(sink), expectNoCreationRequest = false)

    client.clearEntities()

    // source1, source2
    validateInputsOutputs(queryId, Seq(source1, source2), Seq(sink),
      expectNoCreationRequest = false)

    client.clearEntities()

    // source2, source3
    validateInputsOutputs(queryId, Seq(source2, source3), Seq(sink),
      expectNoCreationRequest = false)

    client.clearEntities()

    // source1, source2
    validateInputsOutputs(queryId, Seq(source1, source2), Seq(sink), expectNoCreationRequest = true)

    client.clearEntities()

    // source1, source2, source3
    validateInputsOutputs(queryId, Seq(source1, source2, source3), Seq(sink),
      expectNoCreationRequest = true)
  }

  test("SAC-253 partial sinks presented in streaming query") {
    val cluster = "cl1"
    val queryId = UUID.randomUUID()

    val topic1 = KafkaTopicInformation("topic1")
    val topic2 = KafkaTopicInformation("topic2")
    val topic3 = KafkaTopicInformation("topic3")
    val topicSource = KafkaTopicInformation("topicSource")

    val source = external.kafkaToEntity(cluster, topicSource)
    val sink1 = external.kafkaToEntity(cluster, topic1)
    val sink2 = external.kafkaToEntity(cluster, topic2)
    val sink3 = external.kafkaToEntity(cluster, topic3)

    // sink1
    validateInputsOutputs(queryId, Seq(source), Seq(sink1), expectNoCreationRequest = false)

    client.clearEntities()

    // sink1, sink2
    validateInputsOutputs(queryId, Seq(source), Seq(sink1, sink2), expectNoCreationRequest = false)

    client.clearEntities()

    // sink2, sink3
    validateInputsOutputs(queryId, Seq(source), Seq(sink2, sink3), expectNoCreationRequest = false)

    client.clearEntities()

    // sink1, sink2
    validateInputsOutputs(queryId, Seq(source), Seq(sink1, sink2), expectNoCreationRequest = true)

    client.clearEntities()

    // sink1, sink2, sink3
    validateInputsOutputs(queryId, Seq(source), Seq(sink1, sink2, sink3),
      expectNoCreationRequest = true)
  }

  private def validateInputsOutputs(
      queryId: UUID,
      sources: Seq[SACAtlasEntityWithDependencies],
      sinks: Seq[SACAtlasEntityWithDependencies],
      expectNoCreationRequest: Boolean): Unit = {
    val process = internal.etlProcessToEntity(sources, sinks, Map())
    sut.requestCreation(Seq(process), Some(queryId))

    if (expectNoCreationRequest) {
      // no entities will be created, as both inputs and outputs are subset of
      // accumulated inputs and outputs
      assert(client.createdEntities.isEmpty)
    } else {
      val allEntities = sources ++ sinks ++ Seq(process)
      assert(client.createdEntities.length === allEntities.length)
      assert(client.createdEntities.toSet === allEntities.map(_.entity).toSet)
    }
  }
}
