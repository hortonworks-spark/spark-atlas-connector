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

import org.apache.atlas.ApplicationProperties
import com.hortonworks.spark.atlas.AtlasClientConf.ConfigEntry

class AtlasClientConf {

  private lazy val configuration = ApplicationProperties.get()

  def set(key: String, value: String): AtlasClientConf = {
    configuration.setProperty(key, value)
    this
  }

  def set(key: ConfigEntry, value: String): AtlasClientConf = {
    configuration.setProperty(key.key, value)
    this
  }

  def get(key: String, defaultValue: String): String = {
    Option(configuration.getProperty(key).asInstanceOf[String]).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(configuration.getProperty(key).asInstanceOf[String])
  }

  def getUrl(key: String): Object = {
    configuration.getProperty(key)
  }

  def get(t: ConfigEntry): String = {
    Option(configuration.getProperty(t.key).asInstanceOf[String]).getOrElse(t.defaultValue)
  }
}

object AtlasClientConf {
  case class ConfigEntry(key: String, defaultValue: String)

  val ATLAS_SPARK_ENABLED = ConfigEntry("atlas.spark.enabled", "true")

  val ATLAS_REST_ENDPOINT = ConfigEntry("atlas.rest.address", "localhost:21000")

  val BLOCKING_QUEUE_CAPACITY = ConfigEntry("atlas.blockQueue.size", "10000")
  val BLOCKING_QUEUE_PUT_TIMEOUT = ConfigEntry("atlas.blockQueue.putTimeout.ms", "3000")

  val CLIENT_TYPE = ConfigEntry("atlas.client.type", "kafka")
  val CLIENT_USERNAME = ConfigEntry("atlas.client.username", "admin")
  val CLIENT_PASSWORD = ConfigEntry("atlas.client.password", "admin123")
  val CLIENT_NUM_RETRIES = ConfigEntry("atlas.client.numRetries", "3")

  val CLUSTER_NAME = ConfigEntry("atlas.cluster.name", "primary")

  val CHECK_MODEL_IN_START = ConfigEntry("atlas.client.checkModelInStart", "true")
}
