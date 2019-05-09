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

package com.hortonworks.spark.atlas.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalatest.Matchers
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.types._
import com.hortonworks.spark.atlas.TestUtils._

class MLPipelineTrackerIT extends BaseResourceIT with Matchers with WithHiveSupport {
  private val atlasClient = new RestAtlasClient(atlasClientConf)

  def clusterName: String = atlasClientConf.get(AtlasClientConf.CLUSTER_NAME)

  def getTableEntity(tableName: String): AtlasEntityWithDependencies = {
    val dbDefinition = createDB("db1", "hdfs:///test/db/db1")
    val sd = createStorageFormat()
    val schema = new StructType()
      .add("user", StringType, false)
      .add("age", IntegerType, true)
    val tableDefinition = createTable("db1", s"$tableName", schema, sd)
    internal.sparkTableToEntity(tableDefinition, clusterName, Some(dbDefinition))
  }

  // Enable it to run integrated test.
  it("pipeline and pipeline model") {
    SparkAtlasModel.checkAndCreateTypes(atlasClient)

    val uri = "hdfs://"
    val pipelineDir = "tmp/pipeline"
    val modelDir = "tmp/model"

    val pipelineDirEntity = internal.mlDirectoryToEntity(uri, pipelineDir)
    val modelDirEntity = internal.mlDirectoryToEntity(uri, modelDir)

    atlasClient.createEntitiesWithDependencies(Seq(pipelineDirEntity, modelDirEntity))

    val df = sparkSession.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    )).toDF("id", "features", "label")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("features_scaled")
      .setMin(0.0)
      .setMax(3.0)
    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(df)

    pipeline.write.overwrite().save(pipelineDir)

    val pipelineEntity = internal.mlPipelineToEntity(pipeline.uid, pipelineDirEntity)

    atlasClient.createEntitiesWithDependencies(Seq(pipelineDirEntity, pipelineEntity))

    val modelEntity = internal.mlModelToEntity(model.uid, modelDirEntity)

    atlasClient.createEntitiesWithDependencies(Seq(modelDirEntity, modelEntity))

    val tableEntities1 = getTableEntity("chris1")
    val tableEntities2 = getTableEntity("chris2")

    atlasClient.createEntitiesWithDependencies(tableEntities1)
    atlasClient.createEntitiesWithDependencies(tableEntities2)

  }
}
