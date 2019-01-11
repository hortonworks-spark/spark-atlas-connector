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
import org.apache.spark.sql.kafka010.atlas.ExtractFromDataSource
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.InternalRowMicroBatchWriter
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.sql.streaming.KafkaTopicInformation
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object CommandsHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(node: InsertIntoHiveTable, qd: QueryDetail): Seq[AtlasEntity] = {
      // source tables entities
      val inputsEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

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
      val inputsEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList

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
      val inputsEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

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
      val inputsEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
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
      val inputsEntities = discoverInputsEntities(qd.qe.sparkPlan, qd.qe.executedPlan)

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
      val inputEntities = child match {
        case r: UnresolvedRelation => prepareEntities(r.tableIdentifier)
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }

      // new view entities
      val viewIdentifier = node.name
      val outputEntities = prepareEntities(viewIdentifier)

      // create process entity
      val inputTableEntity = inputEntities.headOption.toList
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

  object CreateDataSourceTableHarvester extends Harvester[CreateDataSourceTableCommand] {
    override def harvest(node: CreateDataSourceTableCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // only have table entities
      tableToEntities(node.table)
    }
  }

  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(node: SaveIntoDataSourceCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // source table entity
      val inputsEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = node match {
        case SHCEntities(shcEntities) => shcEntities
        case KafkaEntities(kafkaEntities) => kafkaEntities.headOption.getOrElse(Seq.empty)
        case e =>
          logWarn(s"Missing output entities: $e")
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

  def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntities(tableDef)
  }

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "executionTime" -> qd.executionTime.toString,
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }

  private def discoverInputsEntities(
      plan: LogicalPlan,
      executedPlan: SparkPlan): Seq[Seq[AtlasEntity]] = {
    val tChildren = plan.collectLeaves()
    // NOTE: Each element in output should be Sequence which first element represents
    // actual input entity (rest entities can be dependencies of input entity).
    // If multiple inputs are extracted from one Relation, they should be provided like
    // Seq(Seq(entities for first), Seq(entities for second), ...)

    tChildren.flatMap {
      case r: HiveTableRelation => Seq(tableToEntities(r.tableMeta))
      case v: View => Seq(tableToEntities(v.desc))
      case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
        catalogTable.map(tbl => Seq(tableToEntities(tbl))).getOrElse(
          fileRelation.inputFiles.map(file => Seq(external.pathToEntity(file))).toSeq)
      case a: AnalysisBarrier => a.child match {
        case SHCEntities(shcEntities) => Seq(shcEntities)
        case HWCEntities(hwcEntities) => Seq(hwcEntities)
        case KafkaEntities(kafkaEntities) => kafkaEntities
        case e =>
          // SPARK-24867 wraps the whole plan at Spark 2.3.2.
          // TODO: Remove duplicated code here and above.
          e.collectLeaves().flatMap {
            case r: HiveTableRelation => Seq(tableToEntities(r.tableMeta))
            case v: View => Seq(tableToEntities(v.desc))
            case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
              catalogTable.map(tbl => Seq(tableToEntities(tbl))).getOrElse(
                fileRelation.inputFiles.map(file => Seq(external.pathToEntity(file))).toSeq)
            case KafkaEntities(kafkaEntities) => kafkaEntities
            case _ => Seq.empty
          }
      }
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case HWCEntities(hwcEntities) => Seq(hwcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  private def discoverInputsEntities(
      sparkPlan: SparkPlan,
      executedPlan: SparkPlan): Seq[Seq[AtlasEntity]] = {
    // NOTE: Each element in output should be Sequence which first element represents
    // actual input entity (rest entities can be dependencies of input entity).
    // If multiple inputs are extracted from one Relation, they should be provided like
    // Seq(Seq(entities for first), Seq(entities for second), ...)

    sparkPlan.collectLeaves().flatMap {
      case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
        Try {
          val method = h.getClass.getMethod("relation")
          method.setAccessible(true)
          val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
          Seq(tableToEntities(relation.tableMeta))
        }.getOrElse(Seq.empty)

      case f: FileSourceScanExec =>
        f.tableIdentifier.map(tbl => Seq(prepareEntities(tbl))).getOrElse(
          f.relation.location.inputFiles.map(file => Seq(external.pathToEntity(file))).toSeq)
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case HWCEntities(hwcEntities) => Seq(hwcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  object HWCHarvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(node: WriteToDataSourceV2Exec, qd: QueryDetail): Seq[AtlasEntity] = {
      // Source table entity
      val inputsEntities = discoverInputsEntities(qd.qe.sparkPlan, qd.qe.executedPlan)

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
    def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
      case ds: DataSourceV2Relation
          if ds.reader.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ) =>
        import scala.collection.JavaConverters._
        val f = ds.reader.getClass.getDeclaredField("options")
        f.setAccessible(true)
        val options = f.get(ds.reader).asInstanceOf[java.util.Map[String, String]]
        Some(getHWCEntity(options.asScala.toMap))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Seq[AtlasEntity]] = plan match {
      case ds: DataSourceV2ScanExec
          if ds.reader.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ) =>
        import scala.collection.JavaConverters._
        val f = ds.reader.getClass.getDeclaredField("options")
        f.setAccessible(true)
        val options = f.get(ds.reader).asInstanceOf[java.util.Map[String, String]]
        Some(getHWCEntity(options.asScala.toMap))
      case _ => None
    }

    private def getHWCEntity(options: Map[String, String]): Seq[AtlasEntity] = {
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
            external.hwcTableToEntities(db, tableName, clusterName)
          case _: OneRowRelation => Seq.empty
          case n =>
            logWarn(s"Unknown leaf node: $n")
            Seq.empty
        }
      } else {
        val (db, tableName) = getDbTableNames(
          options.getOrElse("default.db", "default"), options.getOrElse("table", ""))
        external.hwcTableToEntities(db, tableName, clusterName)
      }
    }

    def getHWCEntity(r: DataSourceWriter): Seq[AtlasEntity] = r match {
      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_WRITE) =>
        val f = r.getClass.getDeclaredField("options")
        f.setAccessible(true)
        val options = f.get(r).asInstanceOf[java.util.Map[String, String]]
        val (db, tableName) = getDbTableNames(
          options.getOrDefault("default.db", "default"), options.getOrDefault("table", ""))
        external.hwcTableToEntities(db, tableName, clusterName)

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_STREAM_WRITE) =>
        val dbField = r.getClass.getDeclaredField("db")
        dbField.setAccessible(true)
        val db = dbField.get(r).asInstanceOf[String]

        val tableField = r.getClass.getDeclaredField("table")
        tableField.setAccessible(true)
        val table = tableField.get(r).asInstanceOf[String]

        external.hwcTableToEntities(db, table, clusterName)

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.STREAM_WRITE) =>
        val dbField = r.getClass.getDeclaredField("db")
        dbField.setAccessible(true)
        val db = dbField.get(r).asInstanceOf[String]

        val tableField = r.getClass.getDeclaredField("table")
        tableField.setAccessible(true)
        val table = tableField.get(r).asInstanceOf[String]

        external.hwcTableToEntities(db, table, clusterName)

      case w: InternalRowMicroBatchWriter
          if w.createInternalRowWriterFactory()
            .getClass.toString.endsWith(HWCSupport.STREAM_WRITE_FACTORY) =>
        val hwcWriterFactory = w.createInternalRowWriterFactory()
        val dbField = hwcWriterFactory.getClass.getDeclaredField("db")
        dbField.setAccessible(true)
        val db = dbField.get(hwcWriterFactory).asInstanceOf[String]

        val tableField = hwcWriterFactory.getClass.getDeclaredField("table")
        tableField.setAccessible(true)
        val table = tableField.get(hwcWriterFactory).asInstanceOf[String]

        external.hwcTableToEntities(db, table, clusterName)

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

  object SHCEntities {
    private val SHC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.HBaseRelation"

    private val RELATION_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.DefaultSource"

    def unapply(plan: LogicalPlan): Option[Seq[AtlasEntity]] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        Some(getSHCEntity(options))
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(RELATION_PROVIDER_CLASS_NAME) =>
        Some(getSHCEntity(sids.options))
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
        val cluster = options.getOrElse(AtlasClientConf.CLUSTER_NAME.key, clusterName)
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

  object KafkaEntities {
    def unapply(plan: LogicalPlan): Option[Seq[Seq[AtlasEntity]]] = plan match {
      case l: LogicalRelation if ExtractFromDataSource.isKafkaRelation(l.relation) =>
        val topics = ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(l.relation)
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)
      case sids: SaveIntoDataSourceCommand
        if ExtractFromDataSource.isKafkaRelationProvider(sids.dataSource) =>
        Some(Seq(getKafkaEntity(sids.options)))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Seq[Seq[AtlasEntity]]] = plan match {
      case r: RowDataSourceScanExec if ExtractFromDataSource.isKafkaRelation(r.relation) =>
        val topics = ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(r.relation)
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)

      case _ => None
    }

    def getKafkaEntity(options: Map[String, String]): Seq[AtlasEntity] = {
      options.get("topic") match {
        case Some(topic) =>
          val cluster = options.get("kafka." + AtlasClientConf.CLUSTER_NAME.key)
          external.kafkaToEntity(clusterName, KafkaTopicInformation(topic, cluster))

        case _ =>
          // output topic not specified: maybe each output row contains target topic name
          // giving up
          Seq.empty[AtlasEntity]
      }
    }
  }
}
