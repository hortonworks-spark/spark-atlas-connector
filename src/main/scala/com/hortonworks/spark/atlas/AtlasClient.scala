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

package com.hortonworks.spark.atlas

import org.apache.atlas.AtlasClientV2
import org.apache.atlas.utils.AuthenticationUtil
import org.apache.spark.SparkEnv

object AtlasClient {
  lazy val atlasClientConf = AtlasClientConf.fromSparkConf(SparkEnv.get.conf)

  @volatile private var client: AtlasClientV2 = null

  def atlasClient(): AtlasClientV2 = {
    if (client == null) {
      AtlasClient.synchronized {
        if (client == null) {
          if (!AuthenticationUtil.isKerberosAuthenticationEnabled) {
            val basicAuth = Array(atlasClientConf.get(AtlasClientConf.CLIENT_USERNAME),
              atlasClientConf.get(AtlasClientConf.CLIENT_PASSWORD))
            client = new AtlasClientV2(getServerUrl(), basicAuth)
          } else {
            client = new AtlasClientV2(getServerUrl(): _*)
          }
        }
      }
    }

    client
  }

  private def getServerUrl(): Array[String] = {
    atlasClientConf.getOption(AtlasClientConf.ATLAS_REST_ENDPOINT.key).map { url =>
      url.split(",")
    }.getOrElse {
      throw new IllegalArgumentException(s"Fail to get atlas.rest.address, please set " +
        "via spark.atlas.rest.address in SparkConf")
    }
  }
}
