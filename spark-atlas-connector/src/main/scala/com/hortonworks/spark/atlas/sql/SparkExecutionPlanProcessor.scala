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

import com.hortonworks.spark.atlas.sql.CommandsHarvester.WriteToDataSourceV2Harvester
import com.hortonworks.spark.atlas.sql.SparkExecutionPlanProcessor.SinkDataSourceWriter

import scala.collection.convert.Wrappers.SeqWrapper
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import com.hortonworks.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf}
import com.hortonworks.spark.atlas.types.{external, metadata}
import com.hortonworks.spark.atlas.utils.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.SinkProgress


case class QueryDetail(qe: QueryExecution, executionId: Long,
  executionTime: Long, query: Option[String] = None, sink: Option[SinkProgress] = None)

class SparkExecutionPlanProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[QueryDetail] with Logging {

  // TODO: We should handle OVERWRITE to remove the old lineage.
  // TODO: We should consider LLAPRelation later
  override protected def process(qd: QueryDetail): Unit = {
    var outNodes: Seq[SparkPlan] = qd.qe.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    if (qd.sink.isDefined && !outNodes.exists(_.isInstanceOf[WriteToDataSourceV2Exec])) {
      val sink = qd.sink.get

      outNodes ++= Seq(
        WriteToDataSourceV2Exec(
          new MicroBatchWriter(0,
            new SinkDataSourceWriter(sink)), qd.qe.sparkPlan))
    }

    val entities = outNodes.flatMap {
      case r: ExecutedCommandExec =>
        r.cmd match {
          case c: LoadDataCommand =>
            // Case 1. LOAD DATA LOCAL INPATH (from local)
            // Case 2. LOAD DATA INPATH (from HDFS)
            logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
            CommandsHarvester.LoadDataHarvester.harvest(c, qd)

          case c: CreateViewCommand =>
            c.viewType match {
              case PersistedView =>
                logDebug(s"CREATE VIEW AS SELECT query: ${qd.qe}")
                CommandsHarvester.CreateViewHarvester.harvest(c, qd)
              case _ => Seq.empty
            }

          case c: SaveIntoDataSourceCommand =>
            logDebug(s"DATA FRAME SAVE INTO DATA SOURCE: ${qd.qe}")
            CommandsHarvester.SaveIntoDataSourceHarvester.harvest(c, qd)

          case c: CreateTableCommand =>
            logDebug(s"CREATE TABLE USING external source - hive")
            CommandsHarvester.CreateTableHarvester.harvest(c, qd)

          case c: CreateDataSourceTableCommand =>
            logDebug(s"CREATE TABLE USING external source")
            CommandsHarvester.CreateDataSourceTableHarvester.harvest(c, qd)

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
        WriteToDataSourceV2Harvester.harvest(r, qd)

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

      if (conf.get(AtlasClientConf.ATLAS_SPARK_COLUMN_ENABLED).toBoolean) {
        atlasClient.createEntities(entities)
        logDebug(s"Created entities with columns")
      } else {
        // We should handle both cases. The type values will be changed later.
        val dbTypes = Seq(external.HIVE_TABLE_TYPE_STRING, metadata.TABLE_TYPE_STRING)
        val excludedTypes = Seq(external.HIVE_COLUMN_TYPE_STRING, metadata.COLUMN_TYPE_STRING)
        val cleanedEntities = entities
          .filterNot(e => excludedTypes.contains(e.getTypeName))
          .map {
            case e if dbTypes.contains(e.getTypeName) =>
              e.removeAttribute("columns")
              e
            case e if e.getTypeName.equals(metadata.PROCESS_TYPE_STRING) =>
              Seq(e.getAttribute("inputs"), e.getAttribute("outputs")).foreach { list =>
                list.asInstanceOf[SeqWrapper[AtlasEntity]].underlying.foreach { o =>
                  o.removeAttribute("columns")
                }
              }
              e
            case e => e
          }

        atlasClient.createEntities(cleanedEntities)
        logDebug(s"Created entities without columns")
      }
    }

}

object SparkExecutionPlanProcessor {

  class SinkDataSourceWriter(val sinkProgress: SinkProgress) extends StreamWriter {
    override def createWriterFactory(): DataWriterFactory[InternalRow] =
      throw new UnsupportedOperationException("should not reach here!")

    override def commit(messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")

    override def abort(messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")

    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")

    override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")
  }

}
