[![Build Status](https://travis-ci.org/hortonworks-spark/spark-atlas-connector.svg?branch=atlas-1.1)](https://travis-ci.org/hortonworks-spark/spark-atlas-connector)

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

Pre-create Atlas models
=======================

Spark Atlas Connector checks and creates Atlas models when starting up to ensure all necessary models are created before pushing metadata changes. Since it is only needed for the first time, Spark Atlas Connector provides the way to pre-create Atlas models.

Suppose Spark is installed in `<spark dist>` directory and `atlas-application.properties` is placed on `<spark dist>/conf` directory:

```shell
java -cp "<spark dist>/jars/*:<spark dist>/conf:spark-atlas-connector_2.11-0.1.0-SNAPSHOT.jar" com.hortonworks.spark.atlas.types.SparkAtlasModel --interactive-auth
```

The tool will leverage REST client API to request to Atlas which requires authentication. Auth. information is read from `atlas-application.properties`, which should be stored as plain text.
(It would be same if you decide to leverage REST client API in SAC itself.)

Given the approach is insecure regardless of security mode of cluster, we strongly encourage you to pass `--interactive-auth` as parameter, which asks you to input username and password of Atlas interactively.

After running above command, you can set `atlas.client.checkModelInStart=false` in `atlas-application.properties` to skip checking and creating models in Spark Atlas Connector's startup.

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

License
=======

Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
