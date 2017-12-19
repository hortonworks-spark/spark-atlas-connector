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

package com.hortonworks.spark.atlas.types

import scala.collection.JavaConverters._

import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.typedef.{AtlasClassificationDef, AtlasEntityDef, AtlasEnumDef, AtlasStructDef}
import org.scalatest.{BeforeAndAfter, Matchers}

import com.hortonworks.spark.atlas.{AtlasClient, BaseResourceIT, RestAtlasClient}

class SparkAtlasModelIT extends BaseResourceIT with Matchers with BeforeAndAfter {
  private var client: AtlasClient = _


  before(
    if (client == null) {
      client = new RestAtlasClient(atlasClientConf)
    }
  )

  override def afterAll(): Unit = {
    client = null
    super.afterAll()
  }

  it("create Spark Atlas model") {
    // If we already have models defined, we should delete them first.
    val typesDef = AtlasTypeUtil.getTypesDef(
      List.empty[AtlasEnumDef].asJava,
      List.empty[AtlasStructDef].asJava,
      SparkAtlasModel.allTypes.values
        .filter(_.isInstanceOf[AtlasClassificationDef])
        .map(_.asInstanceOf[AtlasClassificationDef])
        .toList
        .asJava,
      SparkAtlasModel.allTypes.values
        .filter(_.isInstanceOf[AtlasEntityDef])
        .map(_.asInstanceOf[AtlasEntityDef])
        .toList
        .asJava)

    // If there's no model defined in Atlas, deletion will throw exception
    try {
      deleteTypesDef(typesDef)
    } catch {
      case _: Throwable => // No op
    }

    // Create new Spark Atlas model
    SparkAtlasModel.checkAndCreateTypes(client)

    // Check if model is existed
    SparkAtlasModel.allTypes.keys.foreach { key =>
      val typeDef = getTypeDef(key)
      typeDef should not be (null)
      typeDef.getName should be (key)
    }
  }

  it("update Spark Atlas model if version number is greater than the old one") {
    // Lower the version number to 0.1 to simulate old model.
    SparkAtlasModel.allTypes.values.foreach { p => p.setTypeVersion("0.1") }
    val typesDef = AtlasTypeUtil.getTypesDef(
      List.empty[AtlasEnumDef].asJava,
      List.empty[AtlasStructDef].asJava,
      SparkAtlasModel.allTypes.values
        .filter(_.isInstanceOf[AtlasClassificationDef])
        .map(_.asInstanceOf[AtlasClassificationDef])
        .toList
        .asJava,
      SparkAtlasModel.allTypes.values
        .filter(_.isInstanceOf[AtlasEntityDef])
        .map(_.asInstanceOf[AtlasEntityDef])
        .toList
        .asJava)
    updateTypesDef(typesDef)

    val groupedTypes = SparkAtlasModel.checkAndGroupTypes(client)
    assert(groupedTypes.entityDefsToUpdate.length > 0)
    assert(groupedTypes.classificationDefsToUpdate.length > 0)

    SparkAtlasModel.allTypes.keys.foreach { key =>
      val typeDef = getTypeDef(key)
      typeDef should not be (null)
      typeDef.getName should be (key)
      typeDef.getTypeVersion should be ("0.1")
    }

    // Change to 1.0 to make this as newer version.
    SparkAtlasModel.allTypes.values.foreach { p => p.setTypeVersion("1.0") }
    SparkAtlasModel.checkAndCreateTypes(client)

    // For now since we will update all types with newer version, so the fetched version should
    // be 1.0 now.
    SparkAtlasModel.allTypes.keys.foreach { key =>
      val typeDef = getTypeDef(key)
      typeDef should not be (null)
      typeDef.getName should be (key)
      typeDef.getTypeVersion should be ("1.0")
    }

    // Using low version number to create again will not take effect.
    SparkAtlasModel.allTypes.values.foreach { p => p.setTypeVersion("0.1") }
    SparkAtlasModel.checkAndCreateTypes(client)
    SparkAtlasModel.allTypes.keys.foreach { key =>
      val typeDef = getTypeDef(key)
      typeDef should not be (null)
      typeDef.getName should be(key)
      typeDef.getTypeVersion should be("1.0")
    }

    SparkAtlasModel.allTypes.values.foreach { p => p.setTypeVersion("1.0") }
  }
}
