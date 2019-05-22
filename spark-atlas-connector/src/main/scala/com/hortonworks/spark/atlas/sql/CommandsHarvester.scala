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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.kafka010.atlas.ExtractFromDataSource
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import com.hortonworks.spark.atlas.{AtlasClientConf, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.sql.SparkExecutionPlanProcessor.SinkDataSourceWriter
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.SparkUtils.sparkSession
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.SinkProgress

object CommandsHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(
        node: InsertIntoHiveTable,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source tables entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table entity
      val outputEntities = Seq(tableToEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object InsertIntoHadoopFsRelationHarvester extends Harvester[InsertIntoHadoopFsRelationCommand] {
    override def harvest(
        node: InsertIntoHadoopFsRelationCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source tables/files entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table/file entity
      val outputEntities = Seq(node.catalogTable.map(tableToEntity(_)).getOrElse(
        external.pathToEntity(node.outputPath.toUri.toString)))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateHiveTableAsSelectHarvester extends Harvester[CreateHiveTableAsSelectCommand] {
    override def harvest(
        node: CreateHiveTableAsSelectCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source tables entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table entity
      val outputEntities = Seq(tableToEntity(node.tableDesc.copy(owner = SparkUtils.currUser())))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateTableHarvester extends Harvester[CreateTableCommand] {
    override def harvest(
        node: CreateTableCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      Seq(tableToEntity(node.table))
    }
  }

  object CreateDataSourceTableAsSelectHarvester
    extends Harvester[CreateDataSourceTableAsSelectCommand] {
    override def harvest(
        node: CreateDataSourceTableAsSelectCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = Seq(tableToEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(
        node: LoadDataCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      val inputEntities = Seq(external.pathToEntity(node.path))
      val outputEntities = Seq(prepareEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object InsertIntoHiveDirHarvester extends Harvester[InsertIntoHiveDirCommand] {
    override def harvest(
        node: InsertIntoHiveDirCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      if (node.storage.locationUri.isEmpty) {
        throw new IllegalStateException("Location URI is illegally empty")
      }

      val inputEntities = discoverInputsEntities(qd.qe.sparkPlan, qd.qe.executedPlan)
      val outputEntities = Seq(external.pathToEntity(node.storage.locationUri.get.toString))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(
        node: CreateViewCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // from table entities
      val child = node.child.asInstanceOf[Project].child
      val inputEntities = child match {
        case r: UnresolvedRelation => Seq(prepareEntity(r.tableIdentifier))
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }

      // new view entities
      val viewIdentifier = node.name
      val outputEntities = Seq(prepareEntity(viewIdentifier))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateDataSourceTableHarvester extends Harvester[CreateDataSourceTableCommand] {
    override def harvest(
        node: CreateDataSourceTableCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // only have table entities
      Seq(tableToEntity(node.table))
    }
  }

  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(
        node: SaveIntoDataSourceCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source table entity
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = node match {
        case SHCEntities(shcEntities) => Seq(shcEntities)
        case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
        case KafkaEntities(kafkaEntities) => kafkaEntities
        case e =>
          logWarn(s"Missing output entities: $e")
          Seq.empty
      }

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object WriteToDataSourceV2Harvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(
        node: WriteToDataSourceV2Exec,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      val outputEntities = node.writer match {
        case w: MicroBatchWriter if w.writer.isInstanceOf[SinkDataSourceWriter] =>
          discoverOutputEntities(w.writer.asInstanceOf[SinkDataSourceWriter].sinkProgress)

        case w => discoverOutputEntities(w)
      }

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  def prepareEntity(tableIdentifier: TableIdentifier): SACAtlasReferenceable = {
    val tableName = SparkUtils.getTableName(tableIdentifier)
    val dbName = SparkUtils.getDatabaseName(tableIdentifier)
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntity(tableDef)
  }

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }

  private def makeProcessEntities(
                                   inputsEntities: Seq[SACAtlasReferenceable],
                                   outputEntities: Seq[SACAtlasReferenceable],
                                   qd: QueryDetail): Seq[SACAtlasReferenceable] = {
    val logMap = getPlanInfo(qd)

    val cleanedOutput = cleanOutput(inputsEntities, outputEntities)

    // ml related cached object
    if (internal.cachedObjects.contains("model_uid")) {
      Seq(internal.updateMLProcessToEntity(inputsEntities, cleanedOutput, logMap))
    } else {
      // create process entity
      Seq(internal.etlProcessToEntity(inputsEntities, cleanedOutput, logMap))
    }
  }

  private def discoverInputsEntities(
      plan: LogicalPlan,
      executedPlan: SparkPlan): Seq[SACAtlasReferenceable] = {
    val tChildren = plan.collectLeaves()
    tChildren.flatMap {
      case r: HiveTableRelation => Seq(tableToEntity(r.tableMeta))
      case v: View => Seq(tableToEntity(v.desc))
      case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
        catalogTable.map(tbl => Seq(tableToEntity(tbl))).getOrElse(
          fileRelation.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq)
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case HWCEntities(hwcEntities) => Seq(hwcEntities)
      case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  private def discoverInputsEntities(
      sparkPlan: SparkPlan,
      executedPlan: SparkPlan): Seq[SACAtlasReferenceable] = {
    sparkPlan.collectLeaves().flatMap {
      case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
        Try {
          val method = h.getClass.getMethod("relation")
          method.setAccessible(true)
          val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
          Seq(tableToEntity(relation.tableMeta))
        }.getOrElse(Seq.empty)

      case f: FileSourceScanExec =>
        f.tableIdentifier.map(tbl => Seq(prepareEntity(tbl))).getOrElse(
          f.relation.location.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq)
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case HWCEntities(hwcEntities) => Seq(hwcEntities)
      case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  private def discoverOutputEntities(sink: SinkProgress): Seq[SACAtlasReferenceable] = {
    if (sink.description.contains("FileSink")) {
      val begin = sink.description.indexOf('[')
      val end = sink.description.indexOf(']')
      val path = sink.description.substring(begin + 1, end)
      logDebug(s"record the streaming query sink output path information $path")
      Seq(external.pathToEntity(path))
    } else if (sink.description.contains("ConsoleSinkProvider")) {
      logInfo(s"do not track the console output as Atlas entity ${sink.description}")
      Seq.empty
    } else {
      Seq.empty
    }
  }

  private def discoverOutputEntities(writer: DataSourceWriter): Seq[SACAtlasReferenceable] = {
    writer match {
      case HWCEntities(hwcEntities) => Seq(hwcEntities)
      case KafkaEntities(kafkaEntities) => Seq(kafkaEntities)
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  object HWCHarvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(
        node: WriteToDataSourceV2Exec,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // Source table entity
      val inputsEntities = discoverInputsEntities(qd.qe.sparkPlan, qd.qe.executedPlan)

      // Supports Spark HWC (destination table entity)
      val outputEntities = HWCEntities.getHWCEntity(node.writer) match {
        case Some(entity) => Seq(entity)
        case None => Seq.empty
      }

      // Creates process entity
      val logMap = getPlanInfo(qd)

      // ML related cached object
      if (internal.cachedObjects.contains("model_uid")) {
        Seq(internal.updateMLProcessToEntity(inputsEntities, outputEntities, logMap))
      } else {
        // Creates process entity
        Seq(internal.etlProcessToEntity(inputsEntities, outputEntities, logMap))
      }
    }
  }

  object HWCEntities extends Logging {
    /**
     * Extracts Atlas entities related with Hive Warehouse Connector plans.
     *
     * Hive Warehouse Connector currently supports four types of operations:
     *   1. SQL / DataFrame Read (batch read)
     *   2. SQL / DataFrame Write (batch write)
     *   3. SQL / DataFrame Write in streaming manner (batch write in streaming)
     *   4. Structured Streaming Write (streaming write)
     *
     * For 1., it is supported by looking logical plans (if available) or physical
     * plans up at `HWCEntities` for every execution plan being processed above
     * when it's possible.
     *
     * For 2. and 3., it checks only when the execution plan is `WriteToDataSourceV2Exec`.
     * It checks the write implementation of DataSourceV2 is HWC or not and dispatches to harvest
     * appropriate entities.
     *
     * For 4., it is same as 2. and 3. but it reuses Kafka harvester to handle input sources
     * under the hood when it dispatches to harvest.
     *
     * See also HCC article, "Integrating Apache Hive with Apache Spark - Hive Warehouse Connector"
     * https://goo.gl/p3EXhz
     */
    object HWCSupport {
      val BATCH_READ_SOURCE =
        "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector"
      val BATCH_WRITE =
        "com.hortonworks.spark.sql.hive.llap.HiveWarehouseDataSourceWriter"
      val BATCH_STREAM_WRITE =
        "com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSourceWriter"
      val STREAM_WRITE =
        "com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSourceWriter"

      def extractFromWriter(
          writer: DataSourceWriter): Option[SACAtlasReferenceable] = writer match {
        case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(BATCH_WRITE) => getHWCEntity(w)

        case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(BATCH_STREAM_WRITE) => getHWCEntity(w)

        case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(STREAM_WRITE) => getHWCEntity(w)

        case w: MicroBatchWriter
          if w.getClass.getMethod("writer").invoke(w)
            .getClass.toString.endsWith(STREAM_WRITE) =>
          extractFromWriter(
            w.getClass.getMethod("writer").invoke(w).asInstanceOf[DataSourceWriter])

        case _ => None
      }
    }

    def unapply(plan: LogicalPlan): Option[SACAtlasReferenceable] = plan match {
      case ds: DataSourceV2Relation
          if ds.source.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ_SOURCE) =>
        getHWCEntity(ds.options)
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[SACAtlasReferenceable] = plan match {
      case ds: DataSourceV2ScanExec
          if ds.source.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_READ_SOURCE) =>
        getHWCEntity(ds.options)
      case _ => None
    }

    def unapply(writer: DataSourceWriter): Option[SACAtlasReferenceable] = {
      HWCSupport.extractFromWriter(writer)
    }

    def getHWCEntity(options: Map[String, String]): Option[SACAtlasReferenceable] = {
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
            Seq(external.hiveTableToReference(db, tableName, clusterName))
          case _: OneRowRelation => Seq.empty
          case n =>
            logWarn(s"Unknown leaf node: $n")
            Seq.empty
        }.headOption
      } else {
        val (db, tableName) = getDbTableNames(
          options.getOrElse("default.db", "default"), options.getOrElse("table", ""))
        Some(external.hiveTableToReference(db, tableName, clusterName))
      }
    }

    def getHWCEntity(r: DataSourceWriter): Option[SACAtlasReferenceable] = r match {
      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_WRITE) =>
        val f = r.getClass.getDeclaredField("options")
        f.setAccessible(true)
        val options = f.get(r).asInstanceOf[java.util.Map[String, String]]
        val (db, tableName) = getDbTableNames(
          options.getOrDefault("default.db", "default"), options.getOrDefault("table", ""))
        Some(external.hiveTableToReference(db, tableName, clusterName))

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.BATCH_STREAM_WRITE) =>
        val dbField = r.getClass.getDeclaredField("db")
        dbField.setAccessible(true)
        val db = dbField.get(r).asInstanceOf[String]

        val tableField = r.getClass.getDeclaredField("table")
        tableField.setAccessible(true)
        val table = tableField.get(r).asInstanceOf[String]

        Some(external.hiveTableToReference(db, table, clusterName))

      case _ if r.getClass.getCanonicalName.endsWith(HWCSupport.STREAM_WRITE) =>
        val dbField = r.getClass.getDeclaredField("db")
        dbField.setAccessible(true)
        val db = dbField.get(r).asInstanceOf[String]

        val tableField = r.getClass.getDeclaredField("table")
        tableField.setAccessible(true)
        val table = tableField.get(r).asInstanceOf[String]

        Some(external.hiveTableToReference(db, table, clusterName))

      case w: MicroBatchWriter
          if w.getClass.getMethod("writer").invoke(w)
            .getClass.toString.endsWith(HWCSupport.STREAM_WRITE) =>
        getHWCEntity(w.getClass.getMethod("writer").invoke(w).asInstanceOf[DataSourceWriter])

      case _ => None
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

    def unapply(plan: LogicalPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(RELATION_PROVIDER_CLASS_NAME) =>
        getSHCEntity(sids.options)
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case _ => None
    }

    def getSHCEntity(options: Map[String, String]): Option[SACAtlasEntityWithDependencies] = {
      if (options.getOrElse("catalog", "") != "") {
        val catalog = options("catalog")
        val cluster = options.getOrElse(AtlasClientConf.CLUSTER_NAME.key, clusterName)
        val jObj = parse(catalog).asInstanceOf[JObject]
        val map = jObj.values
        val tableMeta = map("table").asInstanceOf[Map[String, _]]
        // `asInstanceOf` is required. Otherwise, it fails compilation.
        val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
        val tName = tableMeta("name").asInstanceOf[String]
        Some(external.hbaseTableToEntity(cluster, tName, nSpace))
      } else {
        None
      }
    }
  }

  object KafkaEntities {
    private def convertTopicsToEntities(
        topics: Set[KafkaTopicInformation]): Option[Seq[SACAtlasEntityWithDependencies]] = {
      if (topics.nonEmpty) {
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)
      } else {
        None
      }
    }

    def unapply(plan: LogicalPlan): Option[Seq[SACAtlasEntityWithDependencies]] = plan match {
      case l: LogicalRelation if ExtractFromDataSource.isKafkaRelation(l.relation) =>
        val topics = ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(l.relation)
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)
      case sids: SaveIntoDataSourceCommand
        if ExtractFromDataSource.isKafkaRelationProvider(sids.dataSource) =>
        getKafkaEntity(sids.options).map(Seq(_))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Seq[SACAtlasEntityWithDependencies]] = {
      val topics = plan match {
        case r: RowDataSourceScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(r.relation)
        case r: RDDScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromDataSourceV1(r).toSet
        case r: DataSourceV2ScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromDataSourceV2(r).toSet
        case _ => Set.empty[KafkaTopicInformation]
      }

      convertTopicsToEntities(topics)
    }

    def unapply(r: DataSourceWriter): Option[SACAtlasEntityWithDependencies] = r match {
      case writer: MicroBatchWriter => ExtractFromDataSource.extractTopic(writer) match {
        case Some(topicInformation) => Some(external.kafkaToEntity(clusterName, topicInformation))
        case _ => None
      }

      case _ => None
    }

    def getKafkaEntity(options: Map[String, String]): Option[SACAtlasEntityWithDependencies] = {
      options.get("topic") match {
        case Some(topic) =>
          val cluster = options.get("kafka." + AtlasClientConf.CLUSTER_NAME.key)
          Some(external.kafkaToEntity(clusterName, KafkaTopicInformation(topic, cluster)))

        case _ =>
          // output topic not specified: maybe each output row contains target topic name
          // giving up
          None
      }
    }
  }

  object JDBCEntities {
    private val JDBC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation"

    private val JDBC_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider"

    def unapply(plan: LogicalPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(JDBC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        Some(getJdbcEnity(sids.options))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case _ => None
    }

    private def getJdbcEnity(options: Map[String, String]): SACAtlasEntityWithDependencies = {
      val url = options.getOrElse("url", "")
      val tableName = options.getOrElse("dbtable", "")
      external.rdbmsTableToEntity(url, tableName)
    }
  }
}
