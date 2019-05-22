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

import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.{SACAtlasEntityReference, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import org.apache.atlas.AtlasClient
import org.scalatest.FunSuite

trait TableEntityValidator extends FunSuite {
  def assertTable(
                   ref: SACAtlasReferenceable,
                   dbName: String,
                   tblName: String,
                   clusterName: String,
                   useSparkTable: Boolean): Unit = {
    if (useSparkTable) {
      assertSparkTable(ref, dbName, tblName)
    } else {
      assertHiveTable(ref, dbName, tblName, clusterName)
    }
  }

  def assertTableWithNamePrefix(
                                 ref: SACAtlasReferenceable,
                                 dbName: String,
                                 tblNamePrefix: String,
                                 clusterName: String,
                                 useSparkTable: Boolean): Unit = {
    if (useSparkTable) {
      assertSparkTableWithNamePrefix(ref, dbName, tblNamePrefix)
    } else {
      assertHiveTableWithNamePrefix(ref, dbName, tblNamePrefix, clusterName)
    }
  }

  def assertSparkTable(ref: SACAtlasReferenceable, dbName: String, tblName: String): Unit = {
    assert(ref.isInstanceOf[SACAtlasEntityWithDependencies])
    val entity = ref.asInstanceOf[SACAtlasEntityWithDependencies].entity
    assert(entity.getTypeName === metadata.TABLE_TYPE_STRING)
    assert(entity.getAttribute("name") === tblName)
    assert(entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString
      .endsWith(s"$dbName.$tblName"))
  }

  def assertSparkTableWithNamePrefix(
                                      ref: SACAtlasReferenceable,
                                      dbName: String,
                                      tblNamePrefix: String): Unit = {
    assert(ref.isInstanceOf[SACAtlasEntityWithDependencies])
    val entity = ref.asInstanceOf[SACAtlasEntityWithDependencies].entity
    assert(entity.getTypeName === metadata.TABLE_TYPE_STRING)
    assert(entity.getAttribute("name").toString.startsWith(tblNamePrefix))
    assert(entity.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString
      .contains(s"$dbName.$tblNamePrefix"))
  }

  def assertHiveTable(
                       ref: SACAtlasReferenceable,
                       dbName: String,
                       tblName: String,
                       clusterName: String): Unit = {
    assert(ref.isInstanceOf[SACAtlasEntityReference])
    val outputRef = ref.asInstanceOf[SACAtlasEntityReference]
    assert(outputRef.typeName === external.HIVE_TABLE_TYPE_STRING)
    assert(outputRef.qualifiedName === s"$dbName.$tblName@$clusterName")
  }

  def assertHiveTableWithNamePrefix(
                                     ref: SACAtlasReferenceable,
                                     dbName: String,
                                     tblNamePrefix: String,
                                     clusterName: String): Unit = {
    assert(ref.isInstanceOf[SACAtlasEntityReference])
    val outputRef = ref.asInstanceOf[SACAtlasEntityReference]
    assert(outputRef.typeName === external.HIVE_TABLE_TYPE_STRING)
    assert(outputRef.qualifiedName.startsWith(s"$dbName.$tblNamePrefix"))
    assert(outputRef.qualifiedName.endsWith(s"@$clusterName"))
  }
}
