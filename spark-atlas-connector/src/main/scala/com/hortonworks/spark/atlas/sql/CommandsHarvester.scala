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
import org.apache.spark.sql.execution.{FileRelation, FileSourceScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, CreateViewCommand, LoadDataCommand}
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

  val extractInputEntities = new InputEntities(this)
  import extractInputEntities.toInputTablesEntities

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(node: InsertIntoHiveTable, qd: QueryDetail): Seq[AtlasEntity] = {
      val inputsEntities = node.query.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = tableToEntities(node.table)

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  object InsertIntoHadoopFsRelationHarvester extends Harvester[InsertIntoHadoopFsRelationCommand] {
    override def harvest(node: InsertIntoHadoopFsRelationCommand, qd: QueryDetail)
        : Seq[AtlasEntity] = {
      val inputsEntities = node.query.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case _: LocalRelation =>
          logInfo("Local Relation to store Spark ML pipelineModel")
          Seq.empty
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = node.catalogTable.map(tableToEntities(_)).getOrElse(
        List(external.pathToEntity(node.outputPath.toUri.toString)))

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  object CreateHiveTableAsSelectHarvester extends Harvester[CreateHiveTableAsSelectCommand] {
    override def harvest(
        node: CreateHiveTableAsSelectCommand,
        qd: QueryDetail): Seq[AtlasEntity] = {
      val inputsEntities = node.query.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = tableToEntities(node.tableDesc.copy(owner = SparkUtils.currUser()))

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  object CreateDataSourceTableAsSelectHarvester
    extends Harvester[CreateDataSourceTableAsSelectCommand] {
    override def harvest(
        node: CreateDataSourceTableAsSelectCommand,
        qd: QueryDetail): Seq[AtlasEntity] = {
      val inputsEntities = node.query.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = tableToEntities(node.table)

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(node: LoadDataCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      val inputEntities = external.pathToEntity(node.path) :: Nil

      val outputEntities = prepareEntities(node.table)

      harvestedEntities(inputEntities :: Nil, outputEntities, getPlanInfo(qd))
    }
  }

  object InsertIntoHiveDirHarvester extends Harvester[InsertIntoHiveDirCommand] {
    override def harvest(node: InsertIntoHiveDirCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      if (node.storage.locationUri.isEmpty) {
        throw new IllegalStateException("Location URI is illegally empty")
      }

      val inputsEntities = qd.qe.sparkPlan.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = external.pathToEntity(node.storage.locationUri.get.toString)

      harvestedEntities(inputsEntities, outputEntities :: Nil, getPlanInfo(qd))
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(node: CreateViewCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      val inputEntities = node.child.asInstanceOf[Project].child match {
        case r: UnresolvedRelation => prepareEntities(r.tableIdentifier)
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }

      val outputEntities = prepareEntities(node.name)

      harvestedEntities(inputEntities :: Nil, outputEntities, getPlanInfo(qd))
    }
  }

  object CreateDataSourceTableHarvester extends Harvester[CreateDataSourceTableCommand] {
    override def harvest(node: CreateDataSourceTableCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // only have table entities
      tableToEntities(node.table)
    }
  }


  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(node: SaveIntoDataSourceCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      val inputsEntities = node.query.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = extractInputEntities.shcEntities.getSHCEntity(node.options)

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  object HWCHarvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(node: WriteToDataSourceV2Exec, qd: QueryDetail): Seq[AtlasEntity] = {
      val inputsEntities = qd.qe.sparkPlan.collectLeaves().map {
        case extractInputEntities(entities) => entities
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = extractInputEntities.hwcEntities.getHWCEntity(node.writer)

      harvestedEntities(inputsEntities, outputEntities, getPlanInfo(qd))
    }
  }

  def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntities(tableDef)
  }

  private def harvestedEntities(
      inputsEntities: Seq[Seq[AtlasEntity]],
      outputEntities: Seq[AtlasEntity],
      logMap: Map[String, String]): Seq[AtlasEntity] = {
    // Creates process entity
    val inputTablesEntities = toInputTablesEntities(inputsEntities)
    val outputTableEntities = outputEntities.toList

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

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "executionTime" -> qd.executionTime.toString,
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }
}

class InputEntities(atlas: AtlasEntityUtils) {
  val hiveTableEntities: HiveTableEntities = new HiveTableEntities(atlas)
  val viewEntities: ViewEntities = new ViewEntities(atlas)
  val hwcEntities: HWCEntities = new HWCEntities(atlas)
  val shcEntities: SHCEntities = new SHCEntities(atlas)
  // Other matters. `SparkTableEntities` should be the last.
  val sparkTableEntities: SparkTableEntities = new SparkTableEntities(atlas)

  def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
    case hiveTableEntities(inputEntities) => Some(inputEntities)
    case viewEntities(inputEntities) => Some(inputEntities)
    case hwcEntities(inputEntities) => Some(inputEntities)
    case shcEntities(inputEntities) => Some(inputEntities)
    case sparkTableEntities(inputEntities) => Some(inputEntities)
    case _ => None
  }

  def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
    case hiveTableEntities(inputEntites) => Some(inputEntites)
    case hwcEntities(inputEntites) => Some(inputEntites)
    case shcEntities(inputEntites) => Some(inputEntites)
    case sparkTableEntities(inputEntites) => Some(inputEntites)
    case _ => None
  }

  def toInputTablesEntities(
      inputsEntities: Seq[Seq[AtlasEntity]]): List[AtlasEntity] = {
    inputsEntities.flatMap {
      case entities if entities.forall(
          entity => entity.getTypeName == external.FS_PATH_TYPE_STRING ||
            entity.getTypeName == external.HDFS_PATH_TYPE_STRING) =>
        // We will use all the paths as input entities.
        entities
      case entities => entities.headOption
    }.toList
  }
}

class HiveTableEntities(atlas: AtlasEntityUtils) {
  def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
    case r: HiveTableRelation => Some(atlas.tableToEntities(r.tableMeta))
    case _ => None
  }

  def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
    case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
      Some(Try {
        val method = h.getClass.getMethod("relation")
        method.setAccessible(true)
        val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
        atlas.tableToEntities(relation.tableMeta)
      }.getOrElse(Seq.empty))
    case _ => None
  }
}

class ViewEntities(atlas: AtlasEntityUtils) {
  def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
    case v: View => Some(atlas.tableToEntities(v.desc))
    case _ => None
  }
}

class SparkTableEntities(atlas: AtlasEntityUtils) {
  def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
    case l: LogicalRelation if l.relation.isInstanceOf[FileRelation] =>
      Some(l.catalogTable.map(atlas.tableToEntities(_)).getOrElse(
        l.relation.asInstanceOf[FileRelation].inputFiles.map(external.pathToEntity).toSeq))
    case l: LogicalRelation =>
      l.catalogTable.map(atlas.tableToEntities(_))
    case _ => None
  }

  def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
    case f: FileSourceScanExec =>
      Some(f.tableIdentifier.map(CommandsHarvester.prepareEntities).getOrElse(
        f.relation.location.inputFiles.map(external.pathToEntity).toSeq))
    case _ => None
  }
}

class HWCEntities(atlas: AtlasEntityUtils) extends Logging {

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
    if (options.contains("query")) {
      val sql = options("query")
      // HACK ALERT! Currently SAC and HWC only know the query that has to be pushed down
      // to Hive side so here we don't know which tables are to be proceeded. To work around,
      // we use Spark's parser driver to identify tables. Once we know the table
      // identifiers by this, it looks up Hive entities to find Hive tables out.
      val parsedPlan = org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan(sql)
      parsedPlan.collectLeaves().flatMap {
        case r: UnresolvedRelation =>
          val db = r.tableIdentifier.database.getOrElse(
            options.getOrElse("default.db", "default"))
          val tableName = r.tableIdentifier.table
          external.hwcTableToEntities(db, tableName, atlas.clusterName)
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }
    } else {
      val (db, tableName) = getDbTableNames(
        options.getOrElse("default.db", "default"), options.getOrElse("table", ""))
      external.hwcTableToEntities(db, tableName, atlas.clusterName)
    }
  }

  def getHWCEntity(r: DataSourceWriter): Seq[AtlasEntity] = r match {
    case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_WRITE) =>
      val f = r.getClass.getDeclaredField("options")
      f.setAccessible(true)
      val options = f.get(r).asInstanceOf[java.util.Map[String, String]]
      val (db, tableName) = getDbTableNames(
        options.getOrDefault("default.db", "default"), options.getOrDefault("table", ""))
      external.hwcTableToEntities(db, tableName, atlas.clusterName)

    case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_STREAM_WRITE) =>
      val dbField = r.getClass.getDeclaredField("db")
      dbField.setAccessible(true)
      val db = dbField.get(r).asInstanceOf[String]

      val tableField = r.getClass.getDeclaredField("table")
      tableField.setAccessible(true)
      val table = tableField.get(r).asInstanceOf[String]

      external.hwcTableToEntities(db, table, atlas.clusterName)

    case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.STREAM_WRITE) =>
      val dbField = r.getClass.getDeclaredField("db")
      dbField.setAccessible(true)
      val db = dbField.get(r).asInstanceOf[String]

      val tableField = r.getClass.getDeclaredField("table")
      tableField.setAccessible(true)
      val table = tableField.get(r).asInstanceOf[String]

      external.hwcTableToEntities(db, table, atlas.clusterName)

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

class SHCEntities(atlas: AtlasEntityUtils) {
  private val SHC_RELATION_CLASS_NAME =
    "org.apache.spark.sql.execution.datasources.hbase.HBaseRelation"

  def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
    case l: LogicalRelation
      if l.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
      val baseRelation = l.relation.asInstanceOf[BaseRelation]
      val options = baseRelation.getClass.getMethod("parameters")
        .invoke(baseRelation).asInstanceOf[Map[String, String]]
      Some(getSHCEntity(options))
    case _ => None
  }

  def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
    case r: RowDataSourceScanExec
      if r.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
      val baseRelation = r.relation.asInstanceOf[BaseRelation]
      val options = baseRelation.getClass.getMethod("parameters")
        .invoke(baseRelation).asInstanceOf[Map[String, String]]
      Some(getSHCEntity(options))
    case _ => None
  }

  def getSHCEntity(options: Map[String, String]): Seq[AtlasEntity] = {
    if (options.getOrElse("catalog", "") != "") {
      val catalog = options("catalog")
      val cluster = options.getOrElse(AtlasClientConf.CLUSTER_NAME.key, atlas.clusterName)
      val jObj = parse(catalog).asInstanceOf[JObject]
      val map = jObj.values
      val tableMeta = map("table").asInstanceOf[Map[String, _]]
      // `asInstanceOf` is required. Otherwise, it fails compilation.
      val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
      val tName = tableMeta("name").asInstanceOf[String]
      external.hbaseTableToEntity(cluster, tName, nSpace)
    } else {
      Seq.empty[AtlasEntity]
    }
  }
}
