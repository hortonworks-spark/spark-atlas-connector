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

package com.hortonworks.spark.atlas.utils

import com.hortonworks.spark.atlas.sql.CommandsHarvester.logWarn

object JdbcUtils {

  private val DB2_PREFIX = "jdbc:db2"
  private val DERBY_PREFIX = "jdbc:derby"
  private val MARIADB_PREFIX = "jdbc:mariadb"
  private val MYSQL_PREFIX = "jdbc:mysql"
  private val ORACLE_PREFIX = "jdbc:oracle"
  private val POSTGRES_PREFIX = "jdbc:postgresql"
  private val SQL_SERVER_PREFIX = "jdbc:sqlserver"
  private val TERADATA_PREFIX = "jdbc:teradata"

  /**
   * Retrieves the database name from the url
   *
   * @param url the url used by the JDBC driver
   * @return
   */
  def getDatabaseName(url: String): String = url match {
    case url if url.startsWith(DB2_PREFIX) => getDatabaseNameEndOfUrl(url)
    case url if url.startsWith(DERBY_PREFIX) => getDatabaseNameDerbyFormat(url)
    case url if url.startsWith(MARIADB_PREFIX) => getDatabaseNameEndOfUrl(url)
    case url if url.startsWith(MYSQL_PREFIX) => getDatabaseNameEndOfUrl(url)
    case url if url.startsWith(ORACLE_PREFIX) => getDatabaseOracleFormat(url)
    case url if url.startsWith(POSTGRES_PREFIX) => getDatabaseNameEndOfUrl(url)
    case url if url.startsWith(SQL_SERVER_PREFIX) => getDatabaseSqlServerFormat(url)
    case url if url.startsWith(TERADATA_PREFIX) => getDatabaseNameTeradataFormat(url)
    case _ =>
      logWarn(s"Unsupported JDBC driver for url: $url")
      ""
  }

  /**
   * Retrieves database name where in hose:port/dbname format
   */
  private def getDatabaseNameEndOfUrl(url: String): String = {
    val parsedUrl = url.substring(url.lastIndexOf("/") + 1)
    if (parsedUrl.contains("?")) {
      return parsedUrl.substring(0, parsedUrl.indexOf("?"))
    }

    parsedUrl
  }

  /**
   * Retrieves the database name based on Derby format
   */
  private def getDatabaseNameDerbyFormat(url: String): String = {
    val parsedUrl = url match {
      case url if url.contains("/") => url.substring(url.lastIndexOf("/") + 1)
      case _ => url.substring(url.lastIndexOf(":") + 1)
    }

    if (parsedUrl.contains(";")) {
      return parsedUrl.substring(0, parsedUrl.indexOf(";"))
    }

    parsedUrl
  }

  /**
   * Retrieves the database name based on Teradata format
   */
  private def getDatabaseNameTeradataFormat(url: String): String = {
    val databaseKey = "/DATABASE="
    val parsedUrl = url.substring(url.indexOf(databaseKey) + databaseKey.length)
    if (parsedUrl.contains("/")) {
      return parsedUrl.substring(0, parsedUrl.indexOf("/"))
    }

    parsedUrl
  }

  /**
   * Retrieves the database name based on Oracle format
   * e.g. jdbc:oracle:thin:@localhost:1521:testdb
   */
  private def getDatabaseOracleFormat(url: String): String = {
    url.substring(url.toUpperCase().lastIndexOf(":") + 1)
  }

  /**
   * Retrieves the database name based on Microsoft SQL Server format
   */
  private def getDatabaseSqlServerFormat(url: String): String = {
    val databaseNameKey = ";databaseName="
    val parsedUrl = url.substring(url.indexOf(databaseNameKey) + databaseNameKey.length)
    if (parsedUrl.contains(";")) {
      return parsedUrl.substring(0, parsedUrl.indexOf(";"))
    }

    parsedUrl
  }

}
