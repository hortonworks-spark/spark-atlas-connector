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
import org.apache.atlas.AtlasConstants
import org.apache.atlas.`type`.AtlasBuiltInTypes.{AtlasBooleanType, AtlasDateType, AtlasLongType, AtlasStringType}
import org.apache.atlas.`type`.{AtlasArrayType, AtlasMapType, AtlasTypeUtil}
import org.apache.atlas.model.typedef.AtlasRelationshipDef.{PropagateTags, RelationshipCategory}
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality

object metadata {
  val METADATA_VERSION = "1.0"
  val DB_TYPE_STRING = "spark_db"
  val STORAGEDESC_TYPE_STRING = "spark_storagedesc"
  val TABLE_TYPE_STRING = "spark_table"
  val TABLE_DB_RELATIONSHIP_TYPE_STRING = "spark_table_db_rel"
  val TABLE_COLUMNS_RELATIONSHIP_TYPE_STRING = "spark_table_columns_rel"
  val TABLE_STORAGEDESC_RELATIONSHIP_TYPE_STRING: String = "spark_table_storagedesc_rel"
  val PROCESS_TYPE_STRING = "spark_process"
  val ML_DIRECTORY_TYPE_STRING = "spark_ml_directory"
  val ML_PIPELINE_TYPE_STRING = "spark_ml_pipeline"
  val ML_MODEL_TYPE_STRING = "spark_ml_model"
  val ML_PIPELINE_DIRECTORY_RELATIONSHIP_TYPE_STRING = "spark_ml_pipeline_directory_rel"
  val ML_MODEL_DIRECTORY_RELATIONSHIP_TYPE_STRING = "spark_ml_model_directory_rel"

  // ========= DB type =========
  val DB_TYPE = AtlasTypeUtil.createClassTypeDef(
    DB_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("description", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("location", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(
      "parameters", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef(
      "ownerType", new AtlasStringType))

  // ========= Storage description type =========
  val STORAGEDESC_TYPE = AtlasTypeUtil.createClassTypeDef(
    STORAGEDESC_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("Referenceable"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("location", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("inputFormat", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("outputFormat", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("serde", new AtlasStringType),
    AtlasTypeUtil.createRequiredAttrDef("compressed", new AtlasBooleanType),
    AtlasTypeUtil.createOptionalAttrDef(
      "parameters", new AtlasMapType(new AtlasStringType, new AtlasStringType)))

  // ========= Table type =========
  val TABLE_TYPE = AtlasTypeUtil.createClassTypeDef(
    TABLE_TYPE_STRING,
    "",
    METADATA_VERSION,
    ImmutableSet.of("DataSet"),
    AtlasTypeUtil.createUniqueRequiredAttrDef(
      "qualifiedName", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("tableType", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("provider", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(
      "partitionColumnNames", new AtlasArrayType(new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef(
      "bucketSpec", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef("owner", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("ownerType", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("createTime", new AtlasDateType),
    AtlasTypeUtil.createOptionalAttrDef(
      "parameters", new AtlasMapType(new AtlasStringType, new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef("comment", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef(
      "unsupportedFeatures", new AtlasArrayType(new AtlasStringType)),
    AtlasTypeUtil.createOptionalAttrDef("viewOriginalText", new AtlasStringType))

  // ========= Table-related relationship types =========
  val TABLE_DB_RELATIONSHIP_TYPE = AtlasTypeUtil.createRelationshipTypeDef(
    TABLE_DB_RELATIONSHIP_TYPE_STRING,
    "",
    METADATA_VERSION,
    RelationshipCategory.ASSOCIATION,
    PropagateTags.ONE_TO_TWO,
    new AtlasRelationshipEndDef(TABLE_TYPE_STRING, "db", Cardinality.SINGLE),
    new AtlasRelationshipEndDef(DB_TYPE_STRING, "tables", Cardinality.SET)
  )

  val TABLE_STORAGEDESC_RELATIONSHIP_TYPE = AtlasTypeUtil.createRelationshipTypeDef(
    TABLE_STORAGEDESC_RELATIONSHIP_TYPE_STRING,
    "",
    METADATA_VERSION,
    RelationshipCategory.ASSOCIATION,
    PropagateTags.ONE_TO_TWO,
    new AtlasRelationshipEndDef(TABLE_TYPE_STRING, "sd", Cardinality.SINGLE),
    new AtlasRelationshipEndDef(STORAGEDESC_TYPE_STRING, "table", Cardinality.SINGLE)
  )

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
    AtlasTypeUtil.createOptionalAttrDef("description", new AtlasStringType),
    AtlasTypeUtil.createOptionalAttrDef("extra", new AtlasStringType))

  // ========= ML-related relationship types =========

  val ML_PIPELINE_DIRECTORY_RELATIONSHIP_TYPE = AtlasTypeUtil.createRelationshipTypeDef(
    ML_PIPELINE_DIRECTORY_RELATIONSHIP_TYPE_STRING,
    "",
    METADATA_VERSION,
    RelationshipCategory.ASSOCIATION,
    PropagateTags.ONE_TO_TWO,
    new AtlasRelationshipEndDef(ML_PIPELINE_TYPE_STRING, "directory", Cardinality.SINGLE),
    new AtlasRelationshipEndDef(ML_DIRECTORY_TYPE_STRING, "pipeline", Cardinality.SINGLE)
  )

  val ML_MODEL_DIRECTORY_RELATIONSHIP_TYPE = AtlasTypeUtil.createRelationshipTypeDef(
    ML_MODEL_DIRECTORY_RELATIONSHIP_TYPE_STRING,
    "",
    METADATA_VERSION,
    RelationshipCategory.ASSOCIATION,
    PropagateTags.ONE_TO_TWO,
    new AtlasRelationshipEndDef(ML_MODEL_TYPE_STRING, "directory", Cardinality.SINGLE),
    new AtlasRelationshipEndDef(ML_DIRECTORY_TYPE_STRING, "model", Cardinality.SINGLE)
  )

}
