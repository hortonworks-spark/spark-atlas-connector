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

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import org.apache.atlas.AtlasConstants
import org.apache.atlas.`type`.AtlasBuiltInTypes.{AtlasBooleanType, AtlasLongType, AtlasStringType}
import org.apache.atlas.`type`.{AtlasArrayType, AtlasMapType, AtlasTypeUtil}
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef

object metadata {
  val METADATA_VERSION = "1.0"
  val DB_TYPE_STRING = "spark_db"
  val STORAGEDESC_TYPE_STRING = "spark_storagedesc"
  val COLUMN_TYPE_STRING = "spark_column"
  val TABLE_TYPE_STRING = "spark_table"
  val PROCESS_TYPE_STRING = "spark_process"
  val ML_DIRECTORY_TYPE_STRING = "spark_ml_directory"
  val ML_PIPELINE_TYPE_STRING = "spark_ml_pipeline"
  val ML_MODEL_TYPE_STRING = "spark_ml_model"

  import external._

  // ========= DB type =========
  val DB_TYPE = AtlasTypeUtil.createClassTypeDef(
    DB_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("description", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("locationUri", FS_PATH_TYPE_STRING),
    AtlasTypeUtil.createOptionalAttrDef(
      "properties", new AtlasMapType(new AtlasStringType, new AtlasStringType)))

  // ========= Storage description type =========
  val STORAGEDESC_TYPE = AtlasTypeUtil.createClassTypeDef(
    STORAGEDESC_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("Referenceable"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("locationUri", FS_PATH_TYPE_STRING),
    AtlasTypeUtil.createOptionalAttrDef("inputFormat", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("outputFormat", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("serde", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("compressed", new AtlasBooleanType),
    AtlasTypeUtil.createOptionalAttrDef(
      "properties", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDefWithConstraint(
      "table",
      TABLE_TYPE_STRING,
      AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF,
      ImmutableMap.of(AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE, "storage")))

  // ========= Column type =========
  val COLUMN_TYPE = AtlasTypeUtil.createClassTypeDef(
    COLUMN_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("type", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("nullable", new AtlasBooleanType),
    AtlasTypeUtil.createOptionalAttrDef("metadata", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDefWithConstraint(
      "table",
      TABLE_TYPE_STRING,
      AtlasConstraintDef.CONSTRAINT_TYPE_INVERSE_REF,
      ImmutableMap.of(AtlasConstraintDef.CONSTRAINT_PARAM_ATTRIBUTE, "spark_schema")))

  // ========= Table type =========
  val TABLE_TYPE = AtlasTypeUtil.createClassTypeDef(
    TABLE_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("database", DB_TYPE_STRING),
    AtlasTypeUtil.createOptionalAttrDef("tableType", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDefWithConstraint(
      "storage", STORAGEDESC_TYPE_STRING, AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF, null),
    AtlasTypeUtil.createOptionalAttrDefWithConstraint(
      "spark_schema",
      "array<spark_column>",
      AtlasConstraintDef.CONSTRAINT_TYPE_OWNED_REF, null),
    AtlasTypeUtil.createOptionalAttrDef("provider", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(
      "partitionColumnNames", new AtlasArrayType(new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef(
      "bucketSpec", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef("owner", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("createTime", new AtlasLongType),
    AtlasTypeUtil.createOptionalAttrDef("lastAccessTime", new AtlasLongType),
    AtlasTypeUtil.createOptionalAttrDef(
      "properties", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef("comment", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(
      "unsupportedFeatures", new AtlasArrayType(new AtlasStringType)))

  // ========= Process type =========
  val PROCESS_TYPE = AtlasTypeUtil.createClassTypeDef(
    PROCESS_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("Process"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("executionId", new AtlasLongType),
    AtlasTypeUtil.createOptionalAttrDef("currUser", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("remoteUser", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("executionTime", new AtlasLongType),
    AtlasTypeUtil.createOptionalAttrDef("details", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("sparkPlanDescription", new AtlasStringType))

  // ========== ML directory type ==========
  val ML_DIRECTORY_TYPE = AtlasTypeUtil.createClassTypeDef(
    ML_DIRECTORY_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("uri", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("directory", new AtlasStringType))

  // ========== ML pipeline type ==========
  val ML_PIPELINE_TYPE = AtlasTypeUtil.createClassTypeDef(
    ML_PIPELINE_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("directory", ML_DIRECTORY_TYPE_STRING),
    AtlasTypeUtil.createOptionalAttrDef("description", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("extra", new AtlasStringType))

  // ========== ML model type ==========
  val ML_MODEL_TYPE = AtlasTypeUtil.createClassTypeDef(
    ML_MODEL_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("directory", ML_DIRECTORY_TYPE_STRING),
    AtlasTypeUtil.createOptionalAttrDef("description", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("extra", new AtlasStringType))

}
