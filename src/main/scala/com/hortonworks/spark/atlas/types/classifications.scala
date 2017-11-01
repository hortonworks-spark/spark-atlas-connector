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

import com.google.common.collect.ImmutableSet
import org.apache.atlas.`type`.AtlasTypeUtil

object classifications {
  val DIMENSION_CLASSIFICATION = "Dimension"
  val FACT_CLASSIFICATION = "Fact"

  val FS_CLASSIFICATION = "FsAccess"
  val JDBC_CLASSIFICATION = "JdbcAccess"
  val KAFKA_CLASSIFICATION = "KafkaAccess"
  val HBASE_CLASSIFICATION = "HBaseAccess"

  val DIMENSION_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
      DIMENSION_CLASSIFICATION,
      "Dimension Classification",
      ImmutableSet.of[String]())

  val FACT_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
    FACT_CLASSIFICATION,
    "Fact Classification",
    ImmutableSet.of[String]())

  val FS_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
    FS_CLASSIFICATION,
    "FileSystem Classification",
    ImmutableSet.of[String]())

  val JDBC_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
    JDBC_CLASSIFICATION,
    "JDBC Classification",
    ImmutableSet.of[String]())

  val KAFKA_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
    KAFKA_CLASSIFICATION,
    "Kafka Classification",
    ImmutableSet.of[String]())

  val HBASE_CLASSIFICATION_DEF = AtlasTypeUtil.createTraitTypeDef(
    HBASE_CLASSIFICATION,
    "HBase Classification",
    ImmutableSet.of[String]())
}
