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

import java.net.URI

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

object TestUtils {
  def createDB(name: String, location: String): CatalogDatabase = {
    CatalogDatabase(name, "", new URI(location), Map.empty)
  }

  def createStorageFormat(
      locationUri: Option[URI] = None,
      inputFormat: Option[String] = None,
      outputFormat: Option[String] = None,
      serd: Option[String] = None,
      compressed: Boolean = false,
      properties: Map[String, String] = Map.empty): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serd, compressed, properties)
  }

  def createTable(
      db: String,
      table: String,
      schema: StructType,
      storage: CatalogStorageFormat,
      isHiveTable: Boolean = false): CatalogTable = {
    CatalogTable(
      TableIdentifier(table, Some(db)),
      CatalogTableType.MANAGED,
      storage,
      schema,
      provider = if (isHiveTable) Some("hive") else None)
  }
}
