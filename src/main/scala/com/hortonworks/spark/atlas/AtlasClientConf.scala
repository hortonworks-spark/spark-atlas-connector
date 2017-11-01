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

import org.apache.spark.SparkConf

import scala.collection.mutable

class AtlasClientConf(loadFromSysProps: Boolean) {

  def this() = this(loadFromSysProps = true)

  private val configMap = new mutable.HashMap[String, String]()

  if (loadFromSysProps) {
    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.atlas")) {
        configMap.put(k.stripPrefix("spark."), v)
      }
    }
  }

  def set(key: String, value: String): AtlasClientConf = {
    configMap.put(key, value)
    this
  }


  def get(key: String, defaultValue: String): String = {
    configMap.getOrElse(key, defaultValue)
  }

  def setAll(confs: Iterable[(String, String)]): AtlasClientConf = {
    confs.foreach { case (k, v) => configMap.put(k, v) }
    this
  }
}

object AtlasClientConf {
  def fromSparkConf(conf: SparkConf): AtlasClientConf = {
    new AtlasClientConf(false).setAll(conf.getAll.filter(_._1.startsWith("spark.atlas")))
  }
}
