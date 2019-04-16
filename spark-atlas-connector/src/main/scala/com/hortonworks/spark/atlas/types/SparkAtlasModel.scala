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
import scala.collection.mutable.ArrayBuffer

import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.model.typedef.{AtlasClassificationDef, AtlasEntityDef, AtlasEnumDef, AtlasStructDef}
import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.SearchFilter

import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf, RestAtlasClient}
import com.hortonworks.spark.atlas.utils.Logging

object SparkAtlasModel extends Logging {
  import metadata._
  import classifications._

  private[atlas] case class GroupedTypesDef(
      classificationDefsToUpdate: List[AtlasClassificationDef],
      classificationDefsToCreate: List[AtlasClassificationDef],
      entityDefsToUpdate: List[AtlasEntityDef],
      entityDefsToCreate: List[AtlasEntityDef])

  val allTypes = Map(
    DB_TYPE_STRING -> DB_TYPE,
    STORAGEDESC_TYPE_STRING -> STORAGEDESC_TYPE,
    COLUMN_TYPE_STRING -> COLUMN_TYPE,
    TABLE_TYPE_STRING -> TABLE_TYPE,
    PROCESS_TYPE_STRING -> PROCESS_TYPE,
    ML_DIRECTORY_TYPE_STRING -> ML_DIRECTORY_TYPE,
    ML_PIPELINE_TYPE_STRING -> ML_PIPELINE_TYPE,
    ML_MODEL_TYPE_STRING -> ML_MODEL_TYPE,
    DIMENSION_CLASSIFICATION -> DIMENSION_CLASSIFICATION_DEF,
    FACT_CLASSIFICATION -> FACT_CLASSIFICATION_DEF,
    FS_CLASSIFICATION -> FS_CLASSIFICATION_DEF,
    JDBC_CLASSIFICATION -> JDBC_CLASSIFICATION_DEF,
    KAFKA_CLASSIFICATION -> KAFKA_CLASSIFICATION_DEF,
    HBASE_CLASSIFICATION -> HBASE_CLASSIFICATION_DEF,
    STREAM_CLASSIFICATION -> STREAM_CLASSIFICATION_DEF)

  def main(args: Array[String]): Unit = {
    val atlasClientConf = new AtlasClientConf

    if (args.length > 0 && args.contains("--interactive-auth")) {
      // input user name and password
      val console = System.console
      val username = console.readLine("Username: ")
      val password = console.readPassword("Password: ")

      if (username != null && password != null) {
        atlasClientConf.set(AtlasClientConf.CLIENT_USERNAME, username.trim)
        atlasClientConf.set(AtlasClientConf.CLIENT_PASSWORD, String.valueOf(password).trim)
      }
    }

    logInfo(s"Authentication information - username " +
      s"${atlasClientConf.get(AtlasClientConf.CLIENT_USERNAME)}")

    val atlasClient = new RestAtlasClient(atlasClientConf)
    checkAndCreateTypes(atlasClient)

    logInfo(s"Spark Atlas model is created")
  }

  def checkAndCreateTypes(atlasClient: AtlasClient): Unit = {
    val groupedTypes = checkAndGroupTypes(atlasClient)

    if (groupedTypes.classificationDefsToUpdate.nonEmpty ||
      groupedTypes.entityDefsToUpdate.nonEmpty) {
      val typeDefs = AtlasTypeUtil.getTypesDef(List.empty[AtlasEnumDef].asJava,
        List.empty[AtlasStructDef].asJava,
        groupedTypes.classificationDefsToUpdate.asJava,
        groupedTypes.entityDefsToUpdate.asJava)

      logInfo(s"Update all the types def: $typeDefs")
      atlasClient.updateAtlasTypeDefs(typeDefs)
    }

    if (groupedTypes.classificationDefsToCreate.nonEmpty ||
      groupedTypes.entityDefsToCreate.nonEmpty) {
       val typeDefs = AtlasTypeUtil.getTypesDef(List.empty[AtlasEnumDef].asJava,
        List.empty[AtlasStructDef].asJava,
        groupedTypes.classificationDefsToCreate.asJava,
        groupedTypes.entityDefsToCreate.asJava)

      logInfo(s"Create all the types def: $typeDefs")
      atlasClient.createAtlasTypeDefs(typeDefs)
    }
  }

  private[atlas] def checkAndGroupTypes(atlasClient: AtlasClient): GroupedTypesDef = {
    val searchParams = new MultivaluedMapImpl()

    val classificationDefsToUpdate = new ArrayBuffer[AtlasClassificationDef]
    val classificationDefsToCreate = new ArrayBuffer[AtlasClassificationDef]
    val entityDefsToUpdate = new ArrayBuffer[AtlasEntityDef]
    val entityDefsToCreate = new ArrayBuffer[AtlasEntityDef]

    allTypes.foreach { case (tpeName, tpeDef) =>
      searchParams.clear()
      searchParams.add(SearchFilter.PARAM_NAME, tpeName)
      val searchDefs = atlasClient.getAtlasTypeDefs(searchParams)

      tpeDef match {
        case i: AtlasClassificationDef =>
          val classificationDefs =
            searchDefs.getClassificationDefs.asScala.filter(_.getName == tpeName)
          if (classificationDefs.isEmpty) {
            logDebug(s"Classification type: $tpeName is not found in Atlas database")
            classificationDefsToCreate.append(i)
          } else if (classificationDefs.forall(_.getTypeVersion.toDouble <
            METADATA_VERSION.toDouble)) {
            logDebug(s"Classification type: $tpeName found in Atlas database is older than the " +
              "defined one")
            classificationDefsToUpdate.append(i)
          }

        case e: AtlasEntityDef =>
          val entityDefs = searchDefs.getEntityDefs.asScala.filter(_.getName == tpeName)
          if (entityDefs.isEmpty) {
            logDebug(s"Entity type: $tpeName is not found in Atlas database")
            entityDefsToCreate.append(e)
          } else if (entityDefs.forall(_.getTypeVersion.toDouble < METADATA_VERSION.toDouble)) {
            logDebug(s"Entity type: $tpeName found in Atlas database is older than the " +
              "defined one")
            entityDefsToUpdate.append(e)
          }
      }
    }

    GroupedTypesDef(
      classificationDefsToUpdate.toList,
      classificationDefsToCreate.toList,
      entityDefsToUpdate.toList,
      entityDefsToCreate.toList)
  }
}
