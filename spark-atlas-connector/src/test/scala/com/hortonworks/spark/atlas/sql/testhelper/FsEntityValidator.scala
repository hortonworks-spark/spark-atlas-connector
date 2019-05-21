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

package com.hortonworks.spark.atlas.sql.testhelper

import java.io.File
import java.util.Locale

import com.hortonworks.spark.atlas.AtlasEntityReadHelper.{getStringAttribute, listAtlasEntitiesAsType}
import com.hortonworks.spark.atlas.{SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.types.external
import org.apache.atlas.model.instance.AtlasEntity
import org.scalatest.FunSuite

trait FsEntityValidator extends FunSuite {

  def findFsEntities(entities: Seq[AtlasEntity], dir: File): Seq[AtlasEntity] = {
    entities.filter { e =>
      getStringAttribute(e, "qualifiedName").toLowerCase(Locale.ROOT).contains(
        dir.getAbsolutePath.toLowerCase(Locale.ROOT))
    }
  }

  def assertEntitiesFsType(
      dirToExpectedCount: Map[File, Int],
      entities: Set[AtlasEntity]): Unit = {
    val fsEntities = listAtlasEntitiesAsType(entities.toSeq, external.FS_PATH_TYPE_STRING)
    assert(fsEntities.size === dirToExpectedCount.values.sum)

    dirToExpectedCount.foreach { case (dir, expectedCnt) =>
      val fsEntitiesFiltered = fsEntities.filter { e =>
        getStringAttribute(e, "qualifiedName").toLowerCase(Locale.ROOT).contains(
          dir.getAbsolutePath.toLowerCase(Locale.ROOT))
      }
      assert(fsEntitiesFiltered.length === expectedCnt)
    }
  }

  def assertFsEntity(ref: SACAtlasReferenceable, path: String): Unit = {
    val inputEntity = ref.asInstanceOf[SACAtlasEntityWithDependencies].entity
    assertFsEntity(inputEntity, path)
  }

  def assertFsEntity(entity: AtlasEntity, path: String): Unit = {
    assert(entity.getTypeName === external.FS_PATH_TYPE_STRING)
    assert(entity.getAttribute("name") === path.toLowerCase)
  }
}
