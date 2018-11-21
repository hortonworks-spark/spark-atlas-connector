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

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.kafka010.atlas.KafkaHarvester
import org.apache.spark.sql.kafka010.KafkaStreamWriter

import com.hortonworks.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf}
import com.hortonworks.spark.atlas.utils.Logging

case class QueryDetail(qe: QueryExecution, executionId: Long,
  executionTime: Long, query: Option[String] = None)

class SparkExecutionPlanProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[QueryDetail] with Logging {

  // TODO: We should handle OVERWRITE to remove the old lineage.
  // TODO: We should consider LLAPRelation later
  override protected def process(qd: QueryDetail): Unit = {
    val entities = qd.qe.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => p
      case p: WriteToDataSourceV2Exec => p
      case p: LeafExecNode => p
    }.flatMap {
      case r: ExecutedCommandExec =>
        r.cmd match {
          case c: LoadDataCommand =>
            // Case 1. LOAD DATA LOCAL INPATH (from local)
            // Case 2. LOAD DATA INPATH (from HDFS)
            logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
            CommandsHarvester.LoadDataHarvester.harvest(c, qd)

          case c: CreateViewCommand =>
            logDebug(s"CREATE VIEW AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateViewHarvester.harvest(c, qd)

          case c: SaveIntoDataSourceCommand =>
            logDebug(s"DATA FRAME SAVE INTO DATA SOURCE: ${qd.qe}")
            CommandsHarvester.SaveIntoDataSourceHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: DataWritingCommandExec =>
        r.cmd match {
          case c: InsertIntoHiveTable =>
            logDebug(s"INSERT INTO HIVE TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHiveTableHarvester.harvest(c, qd)

          case c: InsertIntoHadoopFsRelationCommand =>
            logDebug(s"INSERT INTO SPARK TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(c, qd)

          case c: CreateHiveTableAsSelectCommand =>
            logDebug(s"CREATE TABLE AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(c, qd)

          case c: CreateDataSourceTableAsSelectCommand =>
            logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: WriteToDataSourceV2Exec =>
        r.writer match {
          case w: MicroBatchWriter =>
            try {
              val streamWriter = w.getClass.getMethod("writer").invoke(w)
              streamWriter match {
                case _: KafkaStreamWriter =>
                  // We don't know the overhead of createWriterFactory() for all data sources,
                  // so pay the overhead of reflection instead of calling createWriterFactory,
                  // and call `createWriterFactory()` only if the datasource is spark-sql-kafka.
                  val topic = KafkaHarvester.extractTopic(w)
                  KafkaHarvester.harvest(topic, r, qd)
                case _ => Seq.empty
              }
            } catch {
              case _: NoSuchMethodException =>
                logDebug("Can not get KafkaStreamWriter, so can not create Kafka topic " +
                  s"entities: ${qd.qe}")
                Seq.empty
            }

          case w: DataSourceWriter =>
            HWCSupport.extract(r, qd).getOrElse(Seq.empty)
        }

      case _ =>
        Seq.empty

        // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
        // CASE LLAP:
        //    case r: RowDataSourceScanExec
        //        if (r.relation.getClass.getCanonicalName.endsWith("dd")) =>
        //      println("close hive connection via " + r.relation.getClass.getCanonicalName)

      } ++ {
        qd.qe.sparkPlan match {
          case d: DataWritingCommandExec if d.cmd.isInstanceOf[InsertIntoHiveDirCommand] =>
            CommandsHarvester.InsertIntoHiveDirHarvester.harvest(
              d.cmd.asInstanceOf[InsertIntoHiveDirCommand], qd)

          case _ =>
            Seq.empty
        }
      }

      atlasClient.createEntities(entities)
    }

}

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

  def extract(plan: WriteToDataSourceV2Exec, qd: QueryDetail): Option[Seq[AtlasEntity]] = {
    plan.writer match {
      case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(BATCH_WRITE) =>
        Some(CommandsHarvester.HWCHarvester.harvest(plan, qd))

      case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(BATCH_STREAM_WRITE) =>
        Some(CommandsHarvester.HWCHarvester.harvest(plan, qd))

      case w: DataSourceWriter
          if w.getClass.getCanonicalName.endsWith(STREAM_WRITE) =>
        Some(HWCStreamingHarvester.harvest(plan, qd))

      case _ => None
    }
  }
}
