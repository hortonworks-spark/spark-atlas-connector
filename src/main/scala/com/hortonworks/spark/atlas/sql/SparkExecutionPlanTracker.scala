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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{HiveTableRelation, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.util.QueryExecutionListener

import com.hortonworks.spark.atlas.types.{SparkAtlasModel, AtlasEntityUtils}
import com.hortonworks.spark.atlas.utils.SparkUtils
import com.hortonworks.spark.atlas.{RestAtlasClient, AtlasClientConf}

class SparkExecutionPlanTracker extends QueryExecutionListener {

    // Skeleton to track QueryExecution of Spark SQL/DF

    // For integration testing only
    private lazy val atlasClientConf = new AtlasClientConf()
      .set(AtlasClientConf.CHECK_MODEL_IN_START.key, "false")
    private lazy val atlasClient = new RestAtlasClient(atlasClientConf)

    SparkAtlasModel.checkAndCreateTypes(atlasClient)

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      // TODO: We should consider multiple inputs and multiple outs.
      // TODO: We should handle OVERWRITE to remove the old lineage.
      // TODO: We should consider LLAPRelation later

      val relations = qe.sparkPlan.collect {
        case p: LeafExecNode => p
      }
      relations.foreach {
        case r: ExecutedCommandExec =>
          r.cmd match {
            case c : CreateTableCommand =>
              SparkUtils.logInfo("Table name in CREATE query: " + c.table.identifier.table)
              val tIdentifier = c.table.identifier
              atlasClient.createEntities(prepareEntities(tIdentifier))

            case c: InsertIntoHiveTable =>
              SparkUtils.logInfo("Table name in INSERT query: " + c.table.identifier.table)
              val child = c.query.asInstanceOf[Project].child
              child match {
                // Case 3. INSERT INTO VALUES
                case ch : LocalRelation => println("Insert table from values()")

                // Case 4. INSERT INTO SELECT
                case ch : SubqueryAlias => println("Insert table from select * from")
                  // Prepare input entities
                  val fromTableIdentifier: Option[TableIdentifier]  = ch.child match {
                    case r: View => Some(r.desc.identifier)
                    case r: HiveTableRelation => Some(r.tableMeta.identifier)
                    case _ => None
                  }
                  val inputsEntities = prepareEntities(fromTableIdentifier.get)

                  // Prepare output entities
                  val outTableIdentifier = c.table.identifier
                  val outputsEntities = prepareEntities(outTableIdentifier)

                  // Create process entity
                  val inputTableEntities = Seq(inputsEntities.head)
                  val outputTableEntities =  Seq(outputsEntities.head)
                  val inputTables = Seq(fromTableIdentifier.get.table)
                  val outputTables = Seq(outTableIdentifier.table)
                  val pEntity = AtlasEntityUtils.processToEntity(qe, inputTableEntities.toList,
                    outputTableEntities.toList, inputTables.toList, outputTables.toList)
                  atlasClient.createEntities(inputsEntities ++ outputsEntities ++ Seq(pEntity))

                // Case 8: Multiple fromTables
                case ch : Filter =>
                  // Prepare input entities
                  val lChild = ch.child.asInstanceOf[Join].left.asInstanceOf[SubqueryAlias]
                    .child.asInstanceOf[HiveTableRelation].tableMeta.identifier
                  val lInputs = prepareEntities(lChild)
                  val rChild = ch.child.asInstanceOf[Join].right.asInstanceOf[SubqueryAlias]
                    .child.asInstanceOf[HiveTableRelation].tableMeta.identifier
                  val rInputs = prepareEntities(rChild)
                  val inputsEntities = lInputs ++ rInputs

                  // Prepare output entities
                  val outTableIdentifier = c.table.identifier
                  val outputsEntities = prepareEntities(outTableIdentifier)

                  // Create process entity
                  val inputTableEntities = Seq(lInputs.head,rInputs.head)
                  val outputTableEntities =  Seq(outputsEntities.head)
                  val inputTables = Seq(lChild.table, rChild.table)
                  val outputTables = Seq(outTableIdentifier.table)
                  val pEntity = AtlasEntityUtils.processToEntity(qe, inputTableEntities.toList,
                    outputTableEntities.toList, inputTables.toList, outputTables.toList)
                  atlasClient.createEntities(inputsEntities ++ outputsEntities ++ Seq(pEntity))

                case _ => None
              }

            // Case 6. CREATE TABLE AS SELECT
            case c : CreateHiveTableAsSelectCommand =>
              SparkUtils.logInfo("Table name in CTAS query: "
                + c.tableDesc.asInstanceOf[CatalogTable].identifier.table)

              // source table entity
              val tChild = c.query.asInstanceOf[Project].child.asInstanceOf[SubqueryAlias].child
              val sourceTableIdentifier = tChild match {
                case r: HiveTableRelation => r.tableMeta.asInstanceOf[CatalogTable].identifier
                case r: View => r.desc.identifier
              }
              val inputsEntities = prepareEntities(sourceTableIdentifier)

              // new table entity
              val newTableIdentifier = c.tableDesc.asInstanceOf[CatalogTable].identifier
              val outputsEntities = prepareEntities(newTableIdentifier)

              // create process entity
              val inputTableEntities = Seq(inputsEntities.head)
              val outputTableEntities =  Seq(outputsEntities.head)
              val inputTables = Seq(sourceTableIdentifier.table)
              val outputTables = Seq(newTableIdentifier.table)
              val pEntity = AtlasEntityUtils.processToEntity(qe, inputTableEntities.toList,
                outputTableEntities.toList, inputTables.toList, outputTables.toList)
              atlasClient.createEntities(inputsEntities ++ outputsEntities ++ Seq(pEntity))

            case c : CreateViewCommand =>
              // sql("CREATE view  s_view as select * from sourceTable")
              println("Table name in CreateViewCommand: " +  c.name.table)

              // from table entities
              val child = c.child.asInstanceOf[Project].child
              val fromTableIdentifier = child.asInstanceOf[UnresolvedRelation].tableIdentifier
              val inputsEntities = prepareEntities(fromTableIdentifier)

              // new view entities
              val viewIdentifier = c.name
              val outputsEntities = prepareEntities(viewIdentifier)

              // create process entity
              val inputTableEntities = Seq(inputsEntities.head)
              val outputTableEntities =  Seq(outputsEntities.head)
              val inputTables = Seq(fromTableIdentifier.table)
              val outputTables = Seq(viewIdentifier.table)
              val pEntity = AtlasEntityUtils.processToEntity(qe, inputTableEntities.toList,
                outputTableEntities.toList, inputTables.toList, outputTables.toList)
              atlasClient.createEntities(inputsEntities ++ outputsEntities ++ Seq(pEntity))

            case c : LoadDataCommand =>
              // Case 1. LOAD DATA LOCAL INPATH (from local)
              // Case 2. LOAD DATA INPATH (from HDFS)
              println("Table name in Load (local file) query: " + c.table + c.path)

            case c: CreateDataSourceTableAsSelectCommand =>
              // Case 7. DF.saveAsTable
              println("Table name in saveAsTable query: " + c.table.identifier.table)

            case _ =>
              println("Unknown command")
              None
          }
        case c =>
          None
            // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
            // CASE LLAP:
            //    case r: RowDataSourceScanExec
            //            if (r.relation.getClass.getCanonicalName.endsWith("dd")) =>
            //              println("close hive connection via " + r.relation.getClass.getCanonicalName)
      }
    }

  private def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")

    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    AtlasEntityUtils.tableToEntities(tableDef)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println(s"onFailure: $funcName, $exception")
    println(qe)
  }
}
