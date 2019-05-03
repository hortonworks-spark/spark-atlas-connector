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
import java.util.Date

import com.hortonworks.spark.atlas.sql.KafkaTopicInformation

import scala.collection.JavaConverters._
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.types.StructType
import com.hortonworks.spark.atlas.{AtlasClient, AtlasUtils}
import com.hortonworks.spark.atlas.utils.{JdbcUtils, SparkUtils}


object external {
  // External metadata types used to link with external entities

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"
  val S3_OBJECT_TYPE_STRING = "aws_s3_object"
  val S3_PSEUDO_DIR_TYPE_STRING = "aws_s3_pseudo_dir"
  val S3_BUCKET_TYPE_STRING = "aws_s3_bucket"

  private def entityToObjectId(entity: AtlasEntity,
                               attributeNames: Seq[String] = Seq("qualifiedName")) = {
    import scala.collection.JavaConverters._

    val objectId = new AtlasObjectId(entity.getGuid, entity.getTypeName)

    val attributeValues = attributeNames.map(entity.getAttribute)
    val uniqueAttributes = attributeNames zip attributeValues

    objectId.setUniqueAttributes(uniqueAttributes.toMap.asJava)

    objectId
  }

  private def isS3Schema(schema: String): Boolean = schema.matches("s3[an]?")

  private def extractS3Entities(uri: URI, fsPath: Path): Seq[AtlasEntity] = {
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
    dirEntity.setAttribute("bucket", entityToObjectId(bucketEntity))

    // object
    val objectEntity = new AtlasEntity(S3_OBJECT_TYPE_STRING)
    objectEntity.setAttribute("name", objectName)
    objectEntity.setAttribute("path", path)
    objectEntity.setAttribute("qualifiedName", objectQualifiedName)
    objectEntity.setAttribute("pseudoDirectory", entityToObjectId(dirEntity))

    Seq(objectEntity, dirEntity, bucketEntity)
  }

  def pathToEntities(path: String): Seq[AtlasEntity] = {
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

      Seq(entity)
    } else if (isS3Schema(uri.getScheme)) {
      extractS3Entities(uri, fsPath)
    } else {
      val entity = new AtlasEntity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)

      Seq(entity)
    }

  }

  def resolveURI(path: String): URI = {
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

  def hbaseTableToEntity(cluster: String, tableName: String, nameSpace: String)
      : Seq[AtlasEntity] = {
    val hbaseEntity = new AtlasEntity(HBASE_TABLE_STRING)
    hbaseEntity.setAttribute("qualifiedName",
      getTableQualifiedName(cluster, nameSpace, tableName))
    hbaseEntity.setAttribute("name", tableName.toLowerCase)
    hbaseEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)
    Seq(hbaseEntity)
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

  def kafkaToEntity(cluster: String, topic: KafkaTopicInformation): Seq[AtlasEntity] = {
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
    Seq(kafkaEntity)
  }

  // ================== Spark's Hive Catalog entities =====================

  // Note: given that we use Spark model types for Hive catalog entities (except HWC),
  // Hive catalog entities should follow Spark model definitions.
  // In Atlas, the attributes which are not in definition are ignored with WARN messages.

  val HIVE_DB_TYPE_STRING = metadata.DB_TYPE_STRING
  val HIVE_STORAGEDESC_TYPE_STRING = metadata.STORAGEDESC_TYPE_STRING
  val HIVE_COLUMN_TYPE_STRING = metadata.COLUMN_TYPE_STRING
  val HIVE_TABLE_TYPE_STRING = metadata.TABLE_TYPE_STRING

  def hiveDbUniqueAttribute(cluster: String, db: String): String = s"${db.toLowerCase}@$cluster"

  def hiveDbToEntities(dbDefinition: CatalogDatabase,
                       cluster: String,
                       owner: String): Seq[AtlasEntity] = {
    val dbEntity = new AtlasEntity(HIVE_DB_TYPE_STRING)
    dbEntity.setAttribute("qualifiedName",
      hiveDbUniqueAttribute(cluster, dbDefinition.name.toLowerCase))
    dbEntity.setAttribute("name", dbDefinition.name.toLowerCase)
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    dbEntity.setAttribute("ownerType", "USER")
    Seq(dbEntity)
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
  def rdbmsTableToEntity(url: String, tableName: String) : Seq[AtlasEntity] = {
    val jdbcEntity = new AtlasEntity(RDBMS_TABLE)

    val databaseName = JdbcUtils.getDatabaseName(url)
    jdbcEntity.setAttribute("qualifiedName", getRdbmsQualifiedName(databaseName, tableName))
    jdbcEntity.setAttribute("name", tableName)
    Seq(jdbcEntity)
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

  def hiveStorageDescUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): String = {
    hiveTableUniqueAttribute(cluster, db, table, isTempTable) + "_storage"
  }

  def hiveStorageDescToEntities(
      storageFormat: CatalogStorageFormat,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): Seq[AtlasEntity] = {
    val sdEntity = new AtlasEntity(HIVE_STORAGEDESC_TYPE_STRING)
    sdEntity.setAttribute("qualifiedName",
      hiveStorageDescUniqueAttribute(cluster, db, table, isTempTable))
    storageFormat.locationUri.foreach { u => sdEntity.setAttribute("location", u.toString) }
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)
    Seq(sdEntity)
  }

  def hiveColumnUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      column: String,
      isTempTable: Boolean = false): String = {
    val tableName = hiveTableUniqueAttribute(cluster, db, table, isTempTable)
    val parts = tableName.split("@")
    s"${parts(0)}.${column.toLowerCase}@${parts(1)}"
  }

  def hiveColumnToEntities(
      schema: StructType,
      cluster: String,
      db: String,
      table: String,
      isTempTable: Boolean = false): List[AtlasEntity] = {
    schema.map { struct =>
      val entity = new AtlasEntity(HIVE_COLUMN_TYPE_STRING)

      entity.setAttribute("qualifiedName",
        hiveColumnUniqueAttribute(cluster, db, table, struct.name, isTempTable))
      entity.setAttribute("name", struct.name.toLowerCase)
      entity.setAttribute("type", struct.dataType.typeName)
      entity.setAttribute("comment", struct.getComment())
      entity
    }.toList
  }

  def hiveTableUniqueAttribute(
      cluster: String,
      db: String,
      table: String,
      isTemporary: Boolean = false): String = {
    val tableName = if (isTemporary) {
      if (SessionState.get() != null && SessionState.get().getSessionId != null) {
        s"${table}_temp-${SessionState.get().getSessionId}"
      } else {
        s"${table}_temp-${RandomStringUtils.random(10)}"
      }
    } else {
      table
    }

    s"${db.toLowerCase}.${tableName.toLowerCase}@$cluster"
  }

  def hiveTableToEntities(
      tblDefinition: CatalogTable,
      cluster: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = tableDefinition.identifier.database.getOrElse("default")
    val table = tableDefinition.identifier.table
    val dbDefinition = mockDbDefinition.getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntities = hiveDbToEntities(dbDefinition, cluster, tableDefinition.owner)
    val sdEntities = hiveStorageDescToEntities(
      tableDefinition.storage, cluster, db, table
      /* isTempTable = false  Spark doesn't support temp table */)
    val schemaEntities = hiveColumnToEntities(
      tableDefinition.schema, cluster, db, table /* , isTempTable = false */)

    val tblEntity = new AtlasEntity(HIVE_TABLE_TYPE_STRING)
    tblEntity.setAttribute("qualifiedName",
      hiveTableUniqueAttribute(cluster, db, table /* , isTemporary = false */))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tableDefinition.viewText.foreach(tblEntity.setAttribute("viewOriginalText", _))
    tblEntity.setAttribute("db", dbEntities.head)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("sd", sdEntities.head)
    tblEntity.setAttribute("columns", schemaEntities.asJava)

    Seq(tblEntity) ++ dbEntities ++ sdEntities ++ schemaEntities
  }

  def hiveTableToEntitiesForAlterTable(
      tblDefinition: CatalogTable,
      cluster: String,
      mockDbDefinition: Option[CatalogDatabase] = None): Seq[AtlasEntity] = {
    val typesToPick = Seq(HIVE_TABLE_TYPE_STRING, HIVE_COLUMN_TYPE_STRING)
    val entities = hiveTableToEntities(tblDefinition, cluster, mockDbDefinition)

    val dbEntity = entities.filter(e => e.getTypeName.equals(HIVE_DB_TYPE_STRING)).head
    val sdEntity = entities.filter(e => e.getTypeName.equals(HIVE_STORAGEDESC_TYPE_STRING)).head
    val tableEntity = entities.filter(e => e.getTypeName.equals(HIVE_TABLE_TYPE_STRING)).head

    // override attribute with reference - Atlas should already have these entities
    tableEntity.setAttribute("db", AtlasUtils.entityToReference(dbEntity, useGuid = false))
    tableEntity.setAttribute("sd", AtlasUtils.entityToReference(sdEntity, useGuid = false))

    entities.filter(e => typesToPick.contains(e.getTypeName))
  }

  // ================== Hive entities (Hive Warehouse Connector) =====================
  val HWC_TABLE_TYPE_STRING = "hive_table"
  val HWC_DB_TYPE_STRING = "hive_db"
  val HWC_STORAGEDESC_TYPE_STRING = "hive_storagedesc"

  def hwcTableUniqueAttribute(
      cluster: String,
      db: String,
      tableName: String): String = {
    s"${db.toLowerCase}.${tableName.toLowerCase}@$cluster"
  }

  def hwcTableToEntities(
      db: String,
      table: String,
      cluster: String): Seq[AtlasEntity] = {

    val dbEntity = new AtlasEntity(HWC_DB_TYPE_STRING)
    dbEntity.setAttribute("qualifiedName",
      hiveDbUniqueAttribute(cluster, db.toLowerCase))
    dbEntity.setAttribute("name", db.toLowerCase)

    val sdEntity = new AtlasEntity(HWC_STORAGEDESC_TYPE_STRING)
    sdEntity.setAttribute("qualifiedName",
      hiveStorageDescUniqueAttribute(cluster, db, table))

    val tblEntity = new AtlasEntity(HWC_TABLE_TYPE_STRING)
    tblEntity.setAttribute("qualifiedName",
      hiveTableUniqueAttribute(cluster, db, table))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("db",
      AtlasUtils.entityToReference(dbEntity, useGuid = false))
    tblEntity.setAttribute("sd",
      AtlasUtils.entityToReference(sdEntity, useGuid = false))

    Seq(tblEntity)
  }
}
