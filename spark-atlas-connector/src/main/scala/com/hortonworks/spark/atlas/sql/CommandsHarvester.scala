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

package com.hortonworks.spark.atlas.sql

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import scala.util.Try

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{FileRelation, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object CommandsHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  // Spark HBase Connector
  private val HBASE_RELATION_CLASS_NAME =
    "org.apache.spark.sql.execution.datasources.hbase.HBaseRelation"

  // Load HBaseRelation class
  lazy val maybeClazz: Option[Class[_]] = {
    try {
      Some(Class.forName(HBASE_RELATION_CLASS_NAME))
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(node: InsertIntoHiveTable, qd: QueryDetail): Seq[AtlasEntity] = {
      // source tables entities
      val tChildren = node.query.collectLeaves()
      val inputsEntities = tChildren.map {
        case r: HiveTableRelation => tableToEntities(r.tableMeta)
        case v: View => tableToEntities(v.desc)
        case l: LogicalRelation => tableToEntities(l.catalogTable.get)
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      // new table entity
      val outputEntities = tableToEntities(node.table)
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = List(outputEntities.head)
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputEntities, logMap)
      } else {
        // create process entity
        // Atlas doesn't support cycle here.
        val cleanedOutput = cleanOutput(inputTablesEntities, outputTableEntities)
        val pEntity = internal.etlProcessToEntity(
          inputTablesEntities, cleanedOutput, logMap)
        Seq(pEntity) ++ inputsEntities.flatten ++ cleanedOutput
      }
    }
  }

  object InsertIntoHadoopFsRelationHarvester extends Harvester[InsertIntoHadoopFsRelationCommand] {
    override def harvest(node: InsertIntoHadoopFsRelationCommand, qd: QueryDetail)
        : Seq[AtlasEntity] = {
      // source tables/files entities
      val tChildren = node.query.collectLeaves()
      var isFiles = false
      val inputsEntities = tChildren.map {
        case r: HiveTableRelation => tableToEntities(r.tableMeta)
        case v: View => tableToEntities(v.desc)
        case l: LogicalRelation if l.catalogTable.isDefined =>
          l.catalogTable.map(tableToEntities(_)).get
        case l: LogicalRelation =>
          isFiles = true
          l.relation match {
            case r: FileRelation => r.inputFiles.map(external.pathToEntity).toSeq
            case _ => Seq.empty
          }
        case HWCEntities(hwcEntities) => hwcEntities
        case local: LocalRelation =>
          logInfo("Local Relation to store Spark ML pipelineModel")
          Seq.empty
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val inputTablesEntities = if (isFiles) inputsEntities.flatten.toList
      else inputsEntities.flatMap(_.headOption).toList

      // new table/file entity
      val outputEntities = node.catalogTable.map(tableToEntities(_)).getOrElse(
        List(external.pathToEntity(node.outputPath.toUri.toString)))
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputEntities, logMap)
      } else {
        val cleanedOutput = cleanOutput(inputTablesEntities, outputEntities)
        val processEntity = internal.etlProcessToEntity(
          inputTablesEntities, cleanedOutput.headOption.toList, logMap)
          Seq(processEntity) ++ inputsEntities.flatten ++ cleanedOutput
        }
    }
  }

  object CreateHiveTableAsSelectHarvester extends Harvester[CreateHiveTableAsSelectCommand] {
    override def harvest(
        node: CreateHiveTableAsSelectCommand,
        qd: QueryDetail): Seq[AtlasEntity] = {
      // source tables entities
      val tChildren = node.query.collectLeaves()
      val inputsEntities = tChildren.map {
        case r: HiveTableRelation => tableToEntities(r.tableMeta)
        case v: View => tableToEntities(v.desc)
        case l: LogicalRelation => l.relation match {
          case r: FileRelation => l.catalogTable.map(tableToEntities(_)).getOrElse(
            l.relation.asInstanceOf[FileRelation].inputFiles.map(external.pathToEntity).toSeq)

          // support SHC
          case r if r.getClass.getCanonicalName.endsWith(HBASE_RELATION_CLASS_NAME) =>
            getHBaseEntity(r)
        }
        case _: OneRowRelation =>
          Seq.empty
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      // new table entity
      val outputEntities = tableToEntities(node.tableDesc.copy(owner = SparkUtils.currUser()))

      // create process entity
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = List(outputEntities.head)
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputEntities, logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          inputTablesEntities, outputTableEntities, logMap)
        Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
      }
    }
  }

  object CreateDataSourceTableAsSelectHarvester
    extends Harvester[CreateDataSourceTableAsSelectCommand] {
    override def harvest(
        node: CreateDataSourceTableAsSelectCommand,
        qd: QueryDetail): Seq[AtlasEntity] = {
      val tChildren = node.query.collectLeaves()
      val inputsEntities = tChildren.map {
        case r: HiveTableRelation => tableToEntities(r.tableMeta)
        case v: View => tableToEntities(v.desc)
        case l: LogicalRelation if l.relation.isInstanceOf[FileRelation] =>
          l.catalogTable.map(tableToEntities(_)).getOrElse {
            l.relation.asInstanceOf[FileRelation].inputFiles.map(external.pathToEntity).toSeq
          }
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }
      val outputEntities = tableToEntities(node.table)
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = List(outputEntities.head)
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputEntities, logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          inputTablesEntities, outputTableEntities, logMap)
        Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
      }
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(node: LoadDataCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      val pathEntity = external.pathToEntity(node.path)
      val outputEntities = prepareEntities(node.table)
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(List(pathEntity), outputEntities, logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          List(pathEntity), List(outputEntities.head), logMap)
        Seq(pEntity, pathEntity) ++ outputEntities
      }
    }
  }

  object InsertIntoHiveDirHarvester extends Harvester[InsertIntoHiveDirCommand] {
    override def harvest(node: InsertIntoHiveDirCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      if (node.storage.locationUri.isEmpty) {
        throw new IllegalStateException("Location URI is illegally empty")
      }

      val destEntity = external.pathToEntity(node.storage.locationUri.get.toString)
      val inputsEntities = qd.qe.sparkPlan.collectLeaves().map {
        case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
          Try {
            val method = h.getClass.getMethod("relation")
            method.setAccessible(true)
            val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
            tableToEntities(relation.tableMeta)
          }.getOrElse(Seq.empty)

        case f: FileSourceScanExec =>
          f.tableIdentifier.map(prepareEntities).getOrElse(
            f.relation.location.inputFiles.map(external.pathToEntity).toSeq)
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val inputs = inputsEntities.flatMap(_.headOption).toList
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputs, List(destEntity), logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          inputs, List(destEntity), logMap)
        Seq(pEntity, destEntity) ++ inputsEntities.flatten
      }
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(node: CreateViewCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // from table entities
      val child = node.child.asInstanceOf[Project].child
      val fromTableIdentifier = child.asInstanceOf[UnresolvedRelation].tableIdentifier
      val inputEntities = prepareEntities(fromTableIdentifier)

      // new view entities
      val viewIdentifier = node.name
      val outputEntities = prepareEntities(viewIdentifier)

      // create process entity
      val inputTableEntity = List(inputEntities.head)
      val outputTableEntity = List(outputEntities.head)
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTableEntity, outputTableEntity, logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          inputTableEntity, outputTableEntity, logMap)
        Seq(pEntity) ++ inputEntities ++ outputEntities
      }
    }
  }

  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(node: SaveIntoDataSourceCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // source table entity
      val tChildren = node.query.collectLeaves()
      val inputsEntities = tChildren.map {
        case r: HiveTableRelation => tableToEntities(r.tableMeta)
        case v: View => tableToEntities(v.desc)
        case l: LogicalRelation => l.relation match {
          case r if r.getClass.getCanonicalName.endsWith(HBASE_RELATION_CLASS_NAME) =>
            getHBaseEntity(r.asInstanceOf[BaseRelation])
          case r: FileRelation => l.catalogTable.map(tableToEntities(_)).getOrElse(
            l.relation.asInstanceOf[FileRelation].inputFiles.map(external.pathToEntity).toSeq)
          case e => Seq.empty
        }
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      // support Spark HBase Connector (destination table entity)
      var catalog = ""
      node.options.foreach {x => if (x._1.equals("catalog")) catalog = x._2}
      val outputEntities = if (catalog != "") {
        val jObj = parse(catalog).asInstanceOf[JObject]
        val map = jObj.values
        val tableMeta = map.get("table").get.asInstanceOf[Map[String, _]]
        val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
        val tName = tableMeta.get("name").get.asInstanceOf[String]
        external.hbaseTableToEntity(conf.get(AtlasClientConf.CLUSTER_NAME), tName, nSpace)
      } else {
        Seq.empty
      }

      // create process entity
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = outputEntities.toList
      val logMap = getPlanInfo(qd)

      // ml related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputTableEntities, logMap)
      } else {

        // create process entity
        val pEntity = internal.etlProcessToEntity(
          inputTablesEntities, outputTableEntities, logMap)
        Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
      }
    }
  }

  private def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntities(tableDef)
  }

  private def getHBaseEntity(r: BaseRelation): Seq[AtlasEntity] = {
    if (maybeClazz.isDefined) {
      val parameters = r.getClass.getMethod("parameters").invoke(r)
      val catalog = parameters.asInstanceOf[Map[String, String]].getOrElse("catalog", "")
      val jObj = parse(catalog).asInstanceOf[JObject]
      val map = jObj.values
      val tableMeta = map.get("table").get.asInstanceOf[Map[String, _]]
      val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
      val tName = tableMeta.get("name").get.asInstanceOf[String]
      external.hbaseTableToEntity(clusterName, tName, nSpace)
    } else {
      logWarn(s"Class $maybeClazz is not found")
      Seq.empty
    }
  }

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "executionTime" -> qd.executionTime.toString,
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }

  object HWCHarvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(node: WriteToDataSourceV2Exec, qd: QueryDetail): Seq[AtlasEntity] = {
      // Source table entity
      val inputsEntities = qd.qe.sparkPlan.collectLeaves().map {
        case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
          Try {
            val method = h.getClass.getMethod("relation")
            method.setAccessible(true)
            val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
            tableToEntities(relation.tableMeta)
          }.getOrElse(Seq.empty)

        case f: FileSourceScanExec =>
          f.tableIdentifier.map(prepareEntities).getOrElse(
            f.relation.location.inputFiles.map(external.pathToEntity).toSeq)
        case HWCEntities(hwcEntities) => hwcEntities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      // Supports Spark HWC (destination table entity)
      val outputEntities = HWCEntities.getHWCEntity(node.writer)

      // Creates process entity
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = outputEntities.toList
      val logMap = getPlanInfo(qd)

      // ML related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        internal.updateMLProcessToEntity(inputTablesEntities, outputTableEntities, logMap)
      } else {
        // Creates process entity
        val pEntity = internal.etlProcessToEntity(
          inputTablesEntities, outputTableEntities, logMap)
        Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
      }
    }
  }

  object HWCEntities extends Logging {
    private def maybeClass(name: String): Option[Class[_]] = try {
      Some(Class.forName(name))
    } catch {
      case _: ClassNotFoundException => None
    }

    private val batchReadSourceClass: Option[Class[_]] =
      maybeClass(HWCSupport.BATCH_READ_SOURCE)

    private val batchWriteClass: Option[Class[_]] = maybeClass(HWCSupport.BATCH_WRITE)
    private val batchStreamWriteClass: Option[Class[_]] =
      maybeClass(HWCSupport.BATCH_STREAM_WRITE)

    private val streamWriteClass: Option[Class[_]] = maybeClass(HWCSupport.STREAM_WRITE)

    def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
      case ds: DataSourceV2Relation
          if ds.source.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ_SOURCE) =>
        Some(getHWCEntity(ds.options))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
      case ds: DataSourceV2ScanExec
          if ds.source.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ_SOURCE) =>
        Some(getHWCEntity(ds.options))
      case _ => None
    }

    def getHWCEntity(options: Map[String, String]): Seq[AtlasEntity] = {
      if (batchReadSourceClass.isDefined) {
        val (db, tableName) = getDbTableNames(
          options.getOrElse("default.db", "default"), options.getOrElse("table", ""))
        external.hwcTableToEntities(db, tableName, clusterName)
      } else {
        logWarn(s"Class ${HWCSupport.BATCH_READ_SOURCE} is not found")
        Seq.empty
      }
    }

    def getHWCEntity(r: DataSourceWriter): Seq[AtlasEntity] = r match {
      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_WRITE) =>
        if (batchWriteClass.isDefined) {
          val f = r.getClass.getDeclaredField("options")
          f.setAccessible(true)
          val options = f.get(r).asInstanceOf[java.util.Map[String, String]]
          val (db, tableName) = getDbTableNames(
            options.getOrDefault("default.db", "default"), options.getOrDefault("table", ""))
          external.hwcTableToEntities(db, tableName, clusterName)
        } else {
          logWarn(s"Class ${HWCSupport.BATCH_WRITE} is not found")
          Seq.empty
        }

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_STREAM_WRITE) =>
        if (batchStreamWriteClass.isDefined) {
          val dbField = r.getClass.getDeclaredField("db")
          dbField.setAccessible(true)
          val db = dbField.get(r).asInstanceOf[String]

          val tableField = r.getClass.getDeclaredField("table")
          tableField.setAccessible(true)
          val table = tableField.get(r).asInstanceOf[String]

          external.hwcTableToEntities(db, table, clusterName)
        } else {
          logWarn(s"Class ${HWCSupport.BATCH_WRITE} is not found")
          Seq.empty
        }

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.STREAM_WRITE) =>
        if (streamWriteClass.isDefined) {
          val dbField = r.getClass.getDeclaredField("db")
          dbField.setAccessible(true)
          val db = dbField.get(r).asInstanceOf[String]

          val tableField = r.getClass.getDeclaredField("table")
          tableField.setAccessible(true)
          val table = tableField.get(r).asInstanceOf[String]

          external.hwcTableToEntities(db, table, clusterName)
        } else {
          logWarn(s"Class ${HWCSupport.STREAM_WRITE} is not found")
          Seq.empty
        }

      case _ => Seq.empty
    }

    // This logic was ported from HWC's `SchemaUtil.getDbTableNames`
    private def getDbTableNames(db: String, nameStr: String): (String, String) = {
      val nameParts = nameStr.split("\\.")
      if (nameParts.length == 1) {
        // hive.table(<unqualified_tableName>) so fill in db from default session db
        (db, nameStr)
      }
      else if (nameParts.length == 2) {
        // hive.table(<qualified_tableName>) so use the provided db
        (nameParts(0), nameParts(1))
      } else {
        throw new IllegalArgumentException(
          "Table name should be specified as either <table> or <db.table>")
      }
    }
  }
}
