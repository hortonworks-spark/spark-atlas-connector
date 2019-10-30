[![Build Status](https://travis-ci.org/hortonworks-spark/spark-atlas-connector.svg?branch=master)](https://travis-ci.org/hortonworks-spark/spark-atlas-connector)

Spark Atlas Connector
===

A connector to track Spark SQL/DataFrame transformations and push metadata changes to Apache Atlas.

This connector supports tracking:

1. SQL DDLs like "CREATE/DROP/ALTER DATABASE", "CREATE/DROP/ALTER TABLE".
2. SQL DMLs like "CREATE TABLE tbl AS SELECT", "INSERT INTO...", "LOAD DATA [LOCAL] INPATH", "INSERT OVERWRITE [LOCAL] DIRECTORY" and so on.
3. DataFrame transformations which has inputs and outputs
4. Machine learning pipelines.

This connector will correlate with other systems like Hive, HDFS to track the life-cycle of data in Atlas.

How To Build
==========

To use this connector, you will require a latest version of Spark (Spark 2.3+), because most of the features only exist in Spark 2.3.0+.

To build this project, please execute:

```shell
mvn package -DskipTests
```

`mvn package` will assemble all the required dependencies and package into an uber jar:

    spark-atlas-connector-assembly/target/spark-atlas-connector-assembly-0.1.0-SNAPSHOT.jar

(`spark-atlas-connector_2.11-0.1.0-SNAPSHOT.jar` is a thin jar without dependencies)

Create Atlas models
===================
NOTE: below steps are only necessary prior to Apache Atlas 2.1.0. Apache Atlas 2.1.0 will include the models. 

SAC leverages official Spark models in Apache Atlas, but as of Apache Atlas 2.0.0, it doesn't include the model file yet. Until Apache Atlas publishes new release which includes the model, SAC includes the json model file to apply to Atlas server easily.

Please copy `1100-spark_model.json` to `<ATLAS_HOME>/models/1000-Hadoop` directory and restart Atlas server to take effect.

How To Use
==========

The connector itself is configured with `atlas-application.properties`.

To get started, you can copy the `atlas-application.properties` from your Atlas server.

## Quick start with Atlas rest client:

Modify your copy of `atlas-application.properties` as shown below.

Set this:

    atlas.client.type=rest

Add credentials. These are the defaults for a vanilla atlas server installation:

    atlas.client.username=admin
    atlas.client.password=admin

If your Atlas server is not on the same host as where your spark job is run:
- Replace `atlas.rest.address=http://localhost:21000` with `http://your-atlas-host:21000`

For production use, consider using `atlas.client.type=kafka` instead.

## Spark config

To use SAC on a spark job, you need to include the uber jar for Spark Driver and set these spark confs:

```
spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
```

For example, to run `spark-shell`:

```shell
bin/spark-shell --jars spark-atlas-connector-assembly-0.1.0-SNAPSHOT.jar \
--conf spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
```

If you're using spark with `--deploy-mode=client` (which is the default):
- Make sure that `atlas-application.properties` is in the Driver's classpath
   - For example, place it at `<SPARK_HOME>/conf/`.

If you're using spark with `--deploy-mode=cluster`:
- Add this spark arg to copy `atlas-application.properties` to all containers:

   `--files atlas-application.properties`

For `--jars` (and `--files`, if applicable), use the full path to the file.
- For example, use an `hdfs://` path for the `spark-atlas-connector-assembly-0.1.0-SNAPSHOT
.jar` if you store the jar on hdfs, etc.

Spark Atlas Connector supports two types of Atlas clients, "kafka" and "rest". You can configure which type of client via setting `atlas.client.type` to whether `kafka` or `rest`.
The default value is `kafka` which provides stable and secured way of publishing changes. Atlas has embedded Kafka instance so you can test it out in test environment, but it's encouraged to use external kafka cluster in production. If you don't have Kafka cluster in production, you may want to set client to `rest`.

To Use it in Secure Environment
===

Atlas now only secures Kafka client API, so when you're using this connector in secure environment, please shift to use Kafka client API by configuring `atlas.client.type=kafka` in `atlas-application.properties`.

Also please add the below configurations to your `atlas-application.properties`.

```
atlas.jaas.KafkaClient.loginModuleControlFlag=required
atlas.jaas.KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule
atlas.jaas.KafkaClient.option.keyTab=./a.keytab
atlas.jaas.KafkaClient.option.principal=spark-test@EXAMPLE.COM
atlas.jaas.KafkaClient.option.serviceName=kafka
atlas.jaas.KafkaClient.option.storeKey=true
atlas.jaas.KafkaClient.option.useKeyTab=true
```

Please make sure keytab (`a.keytab`) is accessible from Spark Driver.

When running on cluster node, you will also need to distribute this keytab, below is the example command to run in cluster mode.

```shell
 ./bin/spark-submit --class <class_name> \
  --jars spark-atlas-connector_2.11-0.1.0-SNAPSHOT.jar \
  --conf spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
  --conf spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
  --conf spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker \
  --master yarn-cluster \
  --principal spark-test@EXAMPLE.COM \
  --keytab ./spark.headless.keytab \
  --files atlas-application.properties,a.keytab \
  <application-jar>
```

When Spark application is started, it will transparently track the execution plan of submitted SQL/DF transformations, parse the plan and create related entities in Atlas.

Spark models vs Hive models
====

SAC classifies table related entities with two different kind of models: Spark / Hive.

We decided to skip sending create events for Hive tables managed by HMS to avoid duplication of those events from Atlas hook for Hive . For Hive entities, Atlas relies on Atlas hook for Hive as the source of truth.

SAC assumes table entities are being created in Hive side and just refers these entities via object id if below conditions are true:

* SparkSession.builder.enableHiveSupport is set
* The value of "hive.metastore.uris" is set to non-empty

For other cases, SAC will create table related entities as Spark models.

One exceptional case is HWC - for HWC source and/or sink, SAC will not create table related entities and always refer to Hive table entities via object id.

Known Limitations (Design decision)
====

> SAC only supports SQL/DataFrame API (in other words, SAC doesn't support RDD).

SAC relies on query listener to retrieve query and examine the impacts.

> All "inputs" and "outputs" in multiple queries are accumulated into single "spark_process" entity when there're multple queries running in single Spark session.

"spark_process" maps to an "applicationId" in Spark. This is helpful as it allows admin to track all changes that occurred as part of an application. But it also causes lineage/relationship graph in "spark_process" to be complicated and less meaningful.

We've filed #261 to investigate changing the unit of "spark_process" entity to query. It doesn't mean we will change it soon. It will be addressed only if we see clear benefits of changing it.

> Only part of inputs are tracked in Streaming query.

This is from design choice on "trade-off": Kafka source supports subscribing with "pattern" and SAC cannot enumerate all matching existing topics, or even all possible topics (even if it was possible, it won't make sense).

"executed plan" provides actual topics which each (micro) batch reads and processes, and as a result, only inputs which participate in (micro) batch are included as "inputs" in "spark_process" entity. 

If your query runs long enough that it ingests data from all topics, it will have all topics in "spark_process" entity.

> SAC doesn't support tracking changes on columns (Spark models).

We are investigating how to add support for column entity. The main issue we face is how to make this change consistent when multiple spark applications make changes to the same table/column.

This doesn't apply to Hive models, which central remote HMS takes care of DDLs and Hive Atlas Hook will take care of updates.

> SAC doesn't track dropping tables (Spark models).

"drop table" event from Spark only provides db and table name, which is NOT sufficient to create qualifierName - especially we separate two types of tables - spark and hive.

SAC depends on reading the Spark Catalog to get table information but Spark will have already dropped the table when SAC notices the table is dropped so that will not work.

We are investigating how to change Spark to provide necessary information via listener, maybe snapshot of information before deletion happens.

> ML entities/events may not be tracked properly.

We are concentrating on making basic features be stable: we are not including ML features on the target of basic features as of now. We will revisit once we are sure to resolve most of issues on basic features.

By the way, we have two patches for tracking ML events: one is a custom patch which could be applied to Spark 2.3/2.4, and another one is a patch which is adopted to Apache Spark but will be available for Spark 3.0. Currently SAC follows custom patch, which is kind of deprecated due to new patch. Maybe we would need to revisit ML features again with Spark 3.0.


License
=======

Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
