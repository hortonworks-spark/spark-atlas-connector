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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.util.control.NonFatal

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.util.QueryExecutionListener

import com.hortonworks.spark.atlas.{AtlasClient, AtlasClientConf}
import com.hortonworks.spark.atlas.utils.Logging

case class QueryDetail(qe: QueryExecution, executionId: Long, executionTime: Long)

class SparkExecutionPlanTracker(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends QueryExecutionListener with AbstractService with Logging {

  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atlasClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt
  // A blocking queue for various query executions
  private val qeQueue = new LinkedBlockingQueue[QueryDetail](capacity)
  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  private val executionId = new AtomicLong(0L)

  startThread()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (!qeQueue.offer(
      QueryDetail(qe, executionId.getAndIncrement(), durationNs), timeout, TimeUnit.MILLISECONDS)) {
      logError(s"Fail to put ${qe.toString()} into queue within time limit $timeout, will throw it")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logWarn(s"Fail to execute query: {$qe}, {$funcName}", exception)
  }

  // TODO: We should consider multiple inputs and multiple outs.
  // TODO: We should handle OVERWRITE to remove the old lineage.
  // TODO: We should consider LLAPRelation later
  override protected def eventProcess(): Unit = {
    var stopped = false
    while (!stopped) {
      try {
        Option(qeQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach { qd =>
          val entities = qd.qe.sparkPlan.collect {
            case p: DataWritingCommandExec => p
            case p: LeafExecNode => p
          }.flatMap {
            case r: ExecutedCommandExec =>
              r.cmd match {
                case c: LoadDataCommand =>
                  // Case 1. LOAD DATA LOCAL INPATH (from local)
                  // Case 2. LOAD DATA INPATH (from HDFS)
                  logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
                  CommandsHarvester.LoadDataHarvester.harvest(c, qd)

                // Case 6. CREATE TABLE AS SELECT
                case c: CreateHiveTableAsSelectCommand =>
                  logDebug(s"CREATE TABLE AS SELECT query: ${qd.qe}")
                  CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(c, qd)

                case c: CreateDataSourceTableAsSelectCommand =>
                  logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
                  CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(c, qd)

                case c: CreateViewCommand =>
                  logDebug(s"CREATE VIEW AS SELECT query: ${qd.qe}")
                  CommandsHarvester.CreateViewHarvester.harvest(c, qd)

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

                case _ =>
                  Seq.empty
              }

            case _ =>
              Seq.empty

              // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
              // CASE LLAP:
              //    case r: RowDataSourceScanExec
              //            if (r.relation.getClass.getCanonicalName.endsWith("dd")) =>
              //              println("close hive connection via " + r.relation.getClass.getCanonicalName)

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
        } catch {
          case _: InterruptedException =>
            logDebug(s"Thread is interrupted")
            stopped = true

          case NonFatal(e) =>
          logWarn(s"Caught exception during parsing the query: $e")
       }
    }
  }
}
