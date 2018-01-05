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
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import com.hortonworks.spark.atlas.AtlasClientConf
import com.hortonworks.spark.atlas.types.AtlasEntityUtils
import com.hortonworks.spark.atlas.utils.SparkUtils

object CommandsHarvester extends AtlasEntityUtils {
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
      // source table entity
      val tChild = node.query.asInstanceOf[Project].child.asInstanceOf[SubqueryAlias].child
      val sourceTableIdentifier = tChild match {
        case r: HiveTableRelation => r.tableMeta.asInstanceOf[CatalogTable].identifier
        case r: View => r.desc.identifier
      }
      val inputEntities = prepareEntities(sourceTableIdentifier)

      // new table entity
      val outputEntities = tableToEntities(node.tableDesc)

      // create process entity
      val inputTableEntities = List(inputEntities.head)
      val outputTableEntities = List(outputEntities.head)
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, inputTableEntities, outputTableEntities)
      Seq(pEntity) ++ inputEntities ++ outputEntities
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(node: CreateViewCommand, qd: QueryDetail): Seq[AtlasEntity] = {
      // from table entities
      val child = node.child.asInstanceOf[Project].child
      val fromTableIdentifier = child.asInstanceOf[UnresolvedRelation].tableIdentifier
      val inputsEntities = prepareEntities(fromTableIdentifier)

      // new view entities
      val viewIdentifier = node.name
      val outputsEntities = prepareEntities(viewIdentifier)

      // create process entity
      val inputTableEntities = List(inputsEntities.head)
      val outputTableEntities = List(outputsEntities.head)
      val pEntity = processToEntity(
        qd.qe, qd.executionId, qd.executionTime, inputTableEntities, outputTableEntities)
      Seq(pEntity) ++ inputsEntities ++ outputsEntities
    }
  }

  private def prepareEntities(tableIdentifier: TableIdentifier): Seq[AtlasEntity] = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntities(tableDef)
  }
}

