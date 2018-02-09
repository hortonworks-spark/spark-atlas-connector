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

import scala.util.Try

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{FileRelation, FileSourceScanExec}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution._

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}

object CommandsHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(node: InsertIntoHiveTable, qd: QueryDetail): Seq[AtlasEntity] = {
      val child = node.query.asInstanceOf[Project].child
      child match {
        // case 3. INSERT INTO VALUES
        case _: LocalRelation =>
          Seq.empty

        // case 4. INSERT INTO SELECT
        case s: SubqueryAlias =>
          // Prepare input entities
          val fromTableIdentifier: Option[TableIdentifier] = s.child match {
            case r: View => Some(r.desc.identifier)
            case r: HiveTableRelation => Some(r.tableMeta.identifier)
            case _ => None
          }
          require(fromTableIdentifier.isDefined, s"Fail to get input table from node $node")
          val inputEntities = prepareEntities(fromTableIdentifier.get)

          // Prepare output entities
          val outTableIdentifier = node.table.identifier
          val outputEntities = prepareEntities(outTableIdentifier)

          // Create process entity
          val inputTableEntity = List(inputEntities.head)
          val outputTableEntity = List(outputEntities.head)
          val pEntity = processToEntity(
            qd.qe, qd.executionId, qd.executionTime, inputTableEntity, outputTableEntity)

          Seq(pEntity) ++ inputEntities ++ outputEntities

        // case 8. Multiple fromTables
        case c: Filter =>
          // Prepare input entities
          val lChild = c.child.asInstanceOf[Join].left.asInstanceOf[SubqueryAlias]
            .child.asInstanceOf[HiveTableRelation].tableMeta.identifier
          val lInputs = prepareEntities(lChild)
          val rChild = c.child.asInstanceOf[Join].right.asInstanceOf[SubqueryAlias]
            .child.asInstanceOf[HiveTableRelation].tableMeta.identifier
          val rInputs = prepareEntities(rChild)
          val inputsEntities = lInputs ++ rInputs

          // Prepare output entities
          val outTableIdentifier = node.table.identifier
          val outputsEntities = prepareEntities(outTableIdentifier)

          // Create process entity
          val inputTableEntities = List(lInputs.head, rInputs.head)
          val outputTableEntities = List(outputsEntities.head)
          val pEntity = processToEntity(
            qd.qe, qd.executionId, qd.executionTime, inputTableEntities, outputTableEntities)

          Seq(pEntity) ++ inputsEntities ++ outputsEntities

        case _ =>
          Seq.empty
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
        case l: LogicalRelation if l.relation.isInstanceOf[FileRelation] =>
          l.catalogTable.map(tableToEntities(_)).getOrElse(
            l.relation.asInstanceOf[FileRelation].inputFiles.map(external.pathToEntity).toSeq)

        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      // new table entity
      val outputEntities = tableToEntities(node.tableDesc)

      // create process entity
      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = List(outputEntities.head)
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, inputTablesEntities, outputTableEntities)
      Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
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

        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val outputEntities = tableToEntities(node.table)

      val inputTablesEntities = inputsEntities.flatMap(_.headOption).toList
      val outputTableEntities = List(outputEntities.head)
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, inputTablesEntities, outputTableEntities)
      Seq(pEntity) ++ inputsEntities.flatten ++ outputEntities
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(node: LoadDataCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      val pathEntity = external.pathToEntity(node.path)
      val outputEntities = prepareEntities(node.table)
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, List(pathEntity), List(outputEntities.head))
      Seq(pEntity, pathEntity) ++ outputEntities
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
        case e =>
          logWarn(s"Missing unknown leaf node: $e")
          Seq.empty
      }

      val inputs = inputsEntities.flatMap(_.headOption).toList
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, inputs, List(destEntity))
      Seq(pEntity, destEntity) ++ inputsEntities.flatten
    }
  }

  private def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntities(tableDef)
  }
}

