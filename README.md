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

`mvn package` will assemble all the required dependencies and package into an uber jar.

Pre-create Atlas models (Recommended)
=====================================

You may need to create Atlas models to ensure all necessary models are created before running SAC and pushing metadata changes. Spark Atlas Connector provides the way to pre-create Atlas models, and it is only needed for the first time.

Suppose Spark is installed in `<SPARK_HOME>` directory and `atlas-application.properties` is placed on `<SPARK_HOME>/conf` directory:

```shell
java -cp "<spark dist>/jars/*:<spark dist>/conf:spark-atlas-connector_2.11-0.1.0-SNAPSHOT.jar" com.hortonworks.spark.atlas.types.SparkAtlasModel --interactive-auth
```

The tool will leverage REST client API to request to Atlas which requires authentication. Auth. information is read from `atlas-application.properties`, which should be stored as plain text.
(It would be same if you decide to leverage REST client API in SAC itself.)

Given the approach is insecure regardless of security mode of cluster, we strongly encourage you to pass `--interactive-auth` as parameter, which asks you to input username and password of Atlas interactively.

If you would like to let SAC handles it instead, you can set you can set `atlas.client.checkModelInStart=true` in `atlas-application.properties` to let SAC check and create models in Spark Atlas Connector's each startup.
Please note that it leverages REST API even you configure `atlas.client.type=kafka`, and it would require additional configuration for authentication. (`atlas.client.username` and `atlas.client.password`)

In the meanwhile, we're also working on migrating models into Apache Atlas: once it's done, we no longer require this step and leverage the models Atlas registers for the startup.

How To Use
==========

To use it, you will need to make this jar accessible in Spark Driver, also configure

```
spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
```

For example, when you're using spark-shell, you can start the Spark like:

```shell
bin/spark-shell --jars spark-atlas-connector_2.11-0.1.0-SNAPSHOT.jar \
--conf spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker \
--conf spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
```

Also make sure atlas configuration file `atlas-application.properties` is in the Driver's classpath. For example, putting this file into `<SPARK_HOME>/conf`.

If you're using cluster mode, please also ship this conf file to the remote Drive using `--files atlas-application.properties`.

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

We decided to determine which models SAC should use via taking realistic condition: whether Hive Hook can take care of table related entities or not. For creating Hive entities, SAC is not the best one to fill out information on entities - so we let Hive to do its own.

As SAC cannot check whether the condition is properly made, SAC relies on precondition: if Spark session is connected to the "remote HMS" (via thrift), SAC assumes Hive Hook is already set up in remote HMS and Hive Hook will take care of entities creation.

In other words, SAC assumes table entities are being created in Hive side and just refers these entities via object id:

* The value of "spark.sql.catalogImplementation" is set to "hive"
* The value of "hive.metastore.uris" is set to non-empty

For other cases, SAC will create necessary table related entities as Spark models (even it uses Hive metastore).

One exceptional case is HWC - for HWC source and/or sink, SAC will not create table related entities and always refer to Hive table entities via object id.

Known Limitations (Design decision)
====

> SAC only supports SQL/DataFrame API (in other words, SAC doesn't support RDD).

SAC relies on query listener to retrieve query and examine the impacts. We may not investigate on supporting RDD for future unless strongly necessary.

> All "inputs" and "outputs" in multiple queries are accumulated into single "spark_process" entity when there're multple queries running in single Spark session.

Unfortunately, we got lost on history for this. We assume this approach could concentrate overall impacts on the single Spark process for who ran for when, but it's not pretty clear why application id was taken.

We've filed #261 to investigate changing the unit of "spark_process" entity to query, but it doesn't mean we will change it soon. It will be address only if we see the clear benefits on changing it.

> Only part of inputs are tracked in Streaming query.

This is from design choice on "trade-off": Kafka source supports subscribing with "pattern" which SAC cannot enumerate all matching existing topics, or even all possible topics (even it's possible it doesn't make sense, though). 

We found "executed plan" provides actual topics which each (micro) batch read and processed, but as a result, only inputs which participate on (micro) batch are being included as "inputs" in "spark_process" entity. 

If your query are running long enough that it ingests data from all topics, it would have all topics in "spark_process" entity.

> SAC doesn't support tracking changes on columns (Spark models).

We found difficulty to make column entity be consistently "up-to-date" supposing multiple spark applications are running and each SAC in spark application are trying to update on Atlas. Due to the difficulty, we dropped supporting column tracking.

We haven't found difficulty on table level, but once we collect difficulties on making table entity being consistent as well, we might also consider drop supporting DDL and blindly create the entities whenever they're referenced.

This doesn't apply to Hive models, which central remote HMS takes care of DDLs and Hive Hook will take care of updates.

> SAC doesn't track dropping tables (Spark models).

Same reason as above. If SAC doesn't support some operations on DDL, it would be basically same reason.

> ML entities/events may not be tracked properly.

We are concentrating on making basic features be stable: we are not including ML features on the target of basic features as of now. We will revisit once we are sure to resolve most of issues on basic features.

By the way, we have two patches for tracking ML events: one is a custom patch which could be applied to Spark 2.3/2.4, and another one is a patch which is adopted to Apache Spark but will be available for Spark 3.0. Currently SAC follows custom patch, which is kind of deprecated due to new patch. Maybe we would need to revisit ML features again with Spark 3.0.


License
=======

Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
