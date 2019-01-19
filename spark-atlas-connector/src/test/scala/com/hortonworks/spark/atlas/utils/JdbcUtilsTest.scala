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

import org.scalatest.{FunSuite, Matchers}

class JdbcUtilsTest extends FunSuite with Matchers {

  test("get database name from mysql url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:mysql://localhost:3306/testdb")
    dbName should be ("testdb")
  }

  test("get database name from mysql url with properties") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:mysql://localhost:3306/testdb?user=root&password=secret")
    dbName should be ("testdb")
  }

  test("get database name from mariadb url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:mariadb://127.0.0.1/testdb")
    dbName should be ("testdb")
  }

  test("get database name from db2 url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:db2://127.0.0.1:50000/testdb")
    dbName should be ("testdb")
  }

  test("get database name from derby url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:derby://localhost/testdb")
    dbName should be ("testdb")
  }

  test("get database name from derby url with properties") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:derby://localhost/testdb;create=true")
    dbName should be ("testdb")
  }

  test("get database name from derby in memory format url with properties") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:derby:memory:testdb;create=true")
    dbName should be ("testdb")
  }

  test("get database name from oracle url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:oracle:thin:root/secret@localhost:1521:testdb")
    dbName should be ("testdb")
  }

  test("get database name from postgres url") {
    val dbName = JdbcUtils.getDatabaseName("jdbc:postgresql://localhost:5432/testdb")
    dbName should be ("testdb")
  }

  test("get database name from sql server url") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:sqlserver://localhost:1433;databaseName=testdb;integratedSecurity=true;")
    dbName should be ("testdb")
  }

  test("get database name from sql server url with properties") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:sqlserver://localhost:1433;databaseName=testdb")
    dbName should be ("testdb")
  }

  test("get database name from teradata url") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:teradata://127.0.0.1/DATABASE=testdb")
    dbName should be ("testdb")
  }

  test("get database name from teradata url with properties") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:teradata://127.0.0.1/DATABASE=testdb/CHARSET=UTF8,COMPAT_DBS=true")
    dbName should be ("testdb")
  }

  test("unsupported database") {
    val dbName = JdbcUtils.getDatabaseName(
      "jdbc:sqlite:product.db")
    dbName should be ("")
  }

}
