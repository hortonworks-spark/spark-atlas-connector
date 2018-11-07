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

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.kafka010.KafkaStreamWriter
import org.apache.spark.sql.kafka010.atlas.KafkaHarvester
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

          case c: CreateDataSourceTableAsSelectCommand =>
            logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(c, qd)

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

          case _ =>
            Seq.empty
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
