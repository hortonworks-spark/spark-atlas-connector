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

package com.hortonworks.spark.atlas.sql.testhelper

import com.hortonworks.spark.atlas.TestUtils
import com.hortonworks.spark.atlas.sql.KafkaTopicInformation
import org.scalatest.FunSuite
import com.hortonworks.spark.atlas.AtlasEntityReadHelper.{getStringAttribute, listAtlasEntitiesAsType}
import com.hortonworks.spark.atlas.types.external.KAFKA_TOPIC_STRING
import org.apache.atlas.model.instance.AtlasEntity

trait KafkaTopicEntityValidator extends FunSuite {

  def assertEntitiesKafkaTopicType(
      topics: Seq[KafkaTopicInformation],
      entities: Set[AtlasEntity]): Unit = {
    val kafkaTopicEntities = listAtlasEntitiesAsType(entities.toSeq, KAFKA_TOPIC_STRING)
    assert(kafkaTopicEntities.size === topics.size)

    val expectedTopicNames = topics.map(_.topicName).toSet
    val expectedClusterNames = topics.map(_.clusterName.getOrElse("primary")).toSet
    val expectedQualifiedNames = topics.map { ti =>
      KafkaTopicInformation.getQualifiedName(ti, "primary")
    }.toSet

    assert(kafkaTopicEntities.map(_.getAttribute("name").toString()).toSet === expectedTopicNames)
    assert(kafkaTopicEntities.map(_.getAttribute("topic").toString()).toSet ===
      expectedTopicNames)
    assert(kafkaTopicEntities.map(getStringAttribute(_, "uri")).toSet === expectedTopicNames)
    assert(kafkaTopicEntities.map(getStringAttribute(_, "clusterName")).toSet ===
      expectedClusterNames)
    assert(kafkaTopicEntities.map(getStringAttribute(_, "qualifiedName")).toSet ===
      expectedQualifiedNames)
  }

  def assertKafkaTopicEntities(
      topics: Seq[KafkaTopicInformation], entities: Seq[AtlasEntity]): Unit = {
    assert(
      topics.map(KafkaTopicInformation.getQualifiedName(_, "primary")).toSet ===
      entities.map(getStringAttribute(_, "qualifiedName")).toSet)
  }

  def assertEntitiesAreSubsetOfTopics(
      topics: Seq[KafkaTopicInformation], entities: Seq[AtlasEntity]): Unit = {
    TestUtils.assertSubsetOf(
      topics.map(KafkaTopicInformation.getQualifiedName(_, "primary")).toSet,
      entities.map(getStringAttribute(_, "qualifiedName")).toSet)
  }
}
