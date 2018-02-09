package com.hortonworks.spark.atlas.utils

import java.net.URI

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType


object CatalogUtils {

  def createDB(name: String, location: String): CatalogDatabase = {
    CatalogDatabase(name, "", new URI(location), Map.empty)
  }

  def createStorageFormat(
                           locationUri: Option[URI] = None,
                           inputFormat: Option[String] = None,
                           outputFormat: Option[String] = None,
                           serd: Option[String] = None,
                           compressed: Boolean = false,
                           properties: Map[String, String] = Map.empty): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serd, compressed, properties)
  }

  def createTable(
                   db: String,
                   table: String,
                   schema: StructType,
                   storage: CatalogStorageFormat,
                   isHiveTable: Boolean = false): CatalogTable = {
    CatalogTable(
      TableIdentifier(table, Some(db)),
      CatalogTableType.MANAGED,
      storage,
      schema,
      provider = if (isHiveTable) Some("hive") else None)
  }

}
