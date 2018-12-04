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

import scala.collection.JavaConverters._

import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.AtlasClientV2
import org.apache.atlas.model.SearchFilter
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.{AtlasStructDef, AtlasTypesDef}
import org.apache.atlas.utils.AuthenticationUtil
import org.scalatest.{BeforeAndAfterAll, FunSuite}

abstract class BaseResourceIT extends FunSuite with BeforeAndAfterAll {

  protected var atlasUrls: Array[String] = null
  private var client: AtlasClientV2 = null
  protected val atlasClientConf = new AtlasClientConf

  override def beforeAll(): Unit = {
    super.beforeAll()


    //set high timeouts so that tests do not fail due to read timeouts while you
    //are stepping through the code in a debugger
    atlasClientConf.set("atlas.client.readTimeoutMSecs", "100000000")
    atlasClientConf.set("atlas.client.connectTimeoutMSecs", "100000000")
    atlasUrls = Array(atlasClientConf.get(AtlasClientConf.ATLAS_REST_ENDPOINT))
  }

  private def atlasClient(): AtlasClientV2 = {
    if (client == null) {
      if (!AuthenticationUtil.isKerberosAuthenticationEnabled) {
        client = new AtlasClientV2(atlasUrls, Array[String]("admin", "admin"))
      } else {
        client = new AtlasClientV2(atlasUrls: _*)
      }
    }

    client
  }

  protected def getTypeDef(name: String): AtlasStructDef = {
    require(atlasClient != null)

    val searchParams = new MultivaluedMapImpl()
    searchParams.add(SearchFilter.PARAM_NAME, name)
    val searchFilter = new SearchFilter(searchParams)
    val typesDef = atlasClient.getAllTypeDefs(searchFilter)
    if (!typesDef.getClassificationDefs.isEmpty) {
      typesDef.getClassificationDefs.get(0)
    } else if (!typesDef.getEntityDefs.isEmpty) {
      typesDef.getEntityDefs.get(0)
    } else {
      null
    }
  }

  protected def updateTypesDef(typesDef: AtlasTypesDef): Unit = {
    require(atlasClient != null)

    atlasClient.updateAtlasTypeDefs(typesDef)
  }

  protected def deleteTypesDef(typesDef: AtlasTypesDef): Unit = {
    require(atlasClient != null)

    atlasClient.deleteAtlasTypeDefs(typesDef)
  }

  protected def getEntity(typeName: String, uniqueAttr: String): AtlasEntity = {
    require(atlasClient != null)

    atlasClient.getEntityByAttribute(typeName,
        Map(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME -> uniqueAttr).asJava)
      .getEntity
  }


  protected def it(desc: String)(testFn: => Unit): Unit = {
    test(desc) {
      assume(
        sys.env.get("ATLAS_INTEGRATION_TEST").contains("true"),
        "integration test can be run only when env ATLAS_INTEGRATION_TEST is set and local Atlas" +
          " is running")
      testFn
    }
  }
}
