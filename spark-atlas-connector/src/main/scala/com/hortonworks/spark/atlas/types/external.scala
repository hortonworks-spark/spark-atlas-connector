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

import java.io.File
import java.net.{URI, URISyntaxException}

import com.hortonworks.spark.atlas.sql.KafkaTopicInformation

import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import com.hortonworks.spark.atlas.{SACAtlasEntityReference, SACAtlasEntityWithDependencies, SACAtlasReferenceable, AtlasUtils}
import com.hortonworks.spark.atlas.utils.{JdbcUtils, SparkUtils}


object external {
  // External metadata types used to link with external entities

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"
  val S3_OBJECT_TYPE_STRING = "aws_s3_object"
  val S3_PSEUDO_DIR_TYPE_STRING = "aws_s3_pseudo_dir"
  val S3_BUCKET_TYPE_STRING = "aws_s3_bucket"

  private def isS3Schema(schema: String): Boolean = schema.matches("s3[an]?")

  private def extractS3Entity(uri: URI, fsPath: Path): SACAtlasEntityWithDependencies = {
    val path = Path.getPathWithoutSchemeAndAuthority(fsPath).toString

    val bucketName = uri.getAuthority
    val bucketQualifiedName = s"s3://${bucketName}"
    val dirName = path.replaceFirst("[^/]*$", "")
    val dirQualifiedName = bucketQualifiedName + dirName
    val objectName = path.replaceFirst("^.*/", "")
    val objectQualifiedName = dirQualifiedName + objectName

    // bucket
    val bucketEntity = new AtlasEntity(S3_BUCKET_TYPE_STRING)
    bucketEntity.setAttribute("name", bucketName)
    bucketEntity.setAttribute("qualifiedName", bucketQualifiedName)

    // pseudo dir
    val dirEntity = new AtlasEntity(S3_PSEUDO_DIR_TYPE_STRING)
    dirEntity.setAttribute("name", dirName)
    dirEntity.setAttribute("qualifiedName", dirQualifiedName)
    dirEntity.setAttribute("objectPrefix", dirQualifiedName)
    dirEntity.setAttribute("bucket", AtlasUtils.entityToReference(bucketEntity))

    // object
    val objectEntity = new AtlasEntity(S3_OBJECT_TYPE_STRING)
    objectEntity.setAttribute("name", objectName)
    objectEntity.setAttribute("path", path)
    objectEntity.setAttribute("qualifiedName", objectQualifiedName)
    objectEntity.setAttribute("pseudoDirectory", AtlasUtils.entityToReference(dirEntity))

    // dir entity depends on bucket entity
    val dirEntityWithDeps = new SACAtlasEntityWithDependencies(dirEntity,
      Seq(SACAtlasEntityWithDependencies(bucketEntity)))

    // object entity depends on dir entity
    new SACAtlasEntityWithDependencies(objectEntity, Seq(dirEntityWithDeps))
  }

  def pathToEntity(path: String): SACAtlasEntityWithDependencies = {
    val uri = resolveURI(path)
    val fsPath = new Path(uri)
    if (uri.getScheme == "hdfs") {
      val entity = new AtlasEntity(HDFS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)

      SACAtlasEntityWithDependencies(entity)
    } else if (isS3Schema(uri.getScheme)) {
      extractS3Entity(uri, fsPath)
    } else {
      val entity = new AtlasEntity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)

      SACAtlasEntityWithDependencies(entity)
    }
  }

  private def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // ================ HBase entities ======================
  val HBASE_NAMESPACE_STRING = "hbase_namespace"
  val HBASE_TABLE_STRING = "hbase_table"
  val HBASE_COLUMNFAMILY_STRING = "hbase_column_family"
  val HBASE_COLUMN_STRING = "hbase_column"
  val HBASE_TABLE_QUALIFIED_NAME_FORMAT = "%s:%s@%s"

  def hbaseTableToEntity(
      cluster: String,
      tableName: String,
      nameSpace: String): SACAtlasEntityWithDependencies = {
    val hbaseEntity = new AtlasEntity(HBASE_TABLE_STRING)
    hbaseEntity.setAttribute("qualifiedName",
      getTableQualifiedName(cluster, nameSpace, tableName))
    hbaseEntity.setAttribute("name", tableName.toLowerCase)
    hbaseEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)

    SACAtlasEntityWithDependencies(hbaseEntity)
  }

  private def getTableQualifiedName(
      clusterName: String,
      nameSpace: String,
      tableName: String): String = {
    if (clusterName == null || nameSpace == null || tableName == null) {
      null
    } else {
      String.format(HBASE_TABLE_QUALIFIED_NAME_FORMAT, nameSpace.toLowerCase,
        tableName.toLowerCase.substring(tableName.toLowerCase.indexOf(":") + 1), clusterName)
    }
  }

  // ================ Kafka entities =======================
  val KAFKA_TOPIC_STRING = "kafka_topic"

  def kafkaToEntity(
      cluster: String,
      topic: KafkaTopicInformation): SACAtlasEntityWithDependencies = {
    val topicName = topic.topicName.toLowerCase
    val clusterName = topic.clusterName match {
      case Some(customName) => customName
      case None => cluster
    }

    val kafkaEntity = new AtlasEntity(KAFKA_TOPIC_STRING)
    kafkaEntity.setAttribute("qualifiedName", topicName + '@' + clusterName)
    kafkaEntity.setAttribute("name", topicName)
    kafkaEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName)
    kafkaEntity.setAttribute("uri", topicName)
    kafkaEntity.setAttribute("topic", topicName)

    SACAtlasEntityWithDependencies(kafkaEntity)
  }

  // ================ RDBMS based entities ======================
  val RDBMS_TABLE = "rdbms_table"

  /**
   * Converts JDBC RDBMS properties into Atlas entity
   *
   * @param url
   * @param tableName
   * @return
   */
  def rdbmsTableToEntity(url: String, tableName: String): SACAtlasEntityWithDependencies = {
    val jdbcEntity = new AtlasEntity(RDBMS_TABLE)

    val databaseName = JdbcUtils.getDatabaseName(url)
    jdbcEntity.setAttribute("qualifiedName", getRdbmsQualifiedName(databaseName, tableName))
    jdbcEntity.setAttribute("name", tableName)

    SACAtlasEntityWithDependencies(jdbcEntity)
  }

  /**
   * Constructs the the full qualified name of the databse
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  private def getRdbmsQualifiedName(databaseName: String, tableName: String): String =
    s"${databaseName.toLowerCase}.${tableName.toLowerCase}"

  // ================== Hive Catalog entities =====================
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  // scalastyle:off
  /**
   * This is based on the logic how Hive Hook defines qualifiedName for Hive DB (borrowed from Apache Atlas v1.1).
   * https://github.com/apache/atlas/blob/release-1.1.0-rc2/addons/hive-bridge/src/main/java/org/apache/atlas/hive/bridge/HiveMetaStoreBridge.java#L833-L841
   *
   * As we cannot guarantee same qualifiedName for temporary table, we just don't support
   * temporary table in SAC.
   */
  // scalastyle:on
  def hiveTableUniqueAttribute(
      cluster: String,
      db: String,
      table: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}@$cluster"
  }

  def hiveTableToReference(
      tblDefinition: CatalogTable,
      cluster: String,
      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    hiveTableToReference(db, table, cluster)
  }

  def hiveTableToReference(
      db: String,
      table: String,
      cluster: String): SACAtlasReferenceable = {
    val qualifiedName = hiveTableUniqueAttribute(cluster, db, table)
    SACAtlasEntityReference(
      new AtlasObjectId(HIVE_TABLE_TYPE_STRING, "qualifiedName", qualifiedName))
  }
}
