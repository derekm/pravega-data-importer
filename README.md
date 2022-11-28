<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Data Importer

Pravega Data Importer is a collection of Apache Flink applications for importing stream data to Pravega from various sources.

It provides the following Flink jobs:

- **stream-mirroring**: Continuously copy a Pravega Stream to another Pravega Stream, even on a different Pravega cluster
- **kafka-stream-mirroring**: Continuously copy a Kafka topic to a Pravega Stream

Each job uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
They automatically recover from failures and resume where they left off.
They can use parallelism for high-volume streams with multiple segments.

To learn more about Pravega, visit http://pravega.io

## Prerequisites

- Java JDK 11+.
  On Ubuntu, this can be installed with:
  ```shell script
  sudo apt-get install openjdk-11-jdk
  ```

- A [Pravega](http://pravega.io) installation
- A [Flink](https://flink.apache.org/) installation

## Stream-Mirroring: Continuously copying a Pravega Stream to another Pravega Stream

### Overview

This Flink job will continuously copy a Pravega Stream to another Pravega Stream.
It uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
It automatically recovers from failures and resumes where it left off.
It can use parallelism for high-volume streams with multiple segments.
It marks the associated input and output Pravega Streams with stream tag `"mirror"`.

### Run Locally

First build project:
```shell
./gradlew clean build installDist -x test
```
Make sure you have a Flink cluster running locally. In the Flink installation directory, you can run the following
command to start a Flink cluster locally:
```shell
bin/start-cluster.sh
```
In the input and output Pravega clusters, make sure the associated stream scope has already been created:
In the Pravega CLI on both source and destination clusters:
```shell
scope create <scope-name>
```
The `help` command in the Pravega CLI gives more information on how to use the Pravega CLI.

Please navigate to this directory once gradle build is complete:
```shell
cd pravega-data-importer/build/install/pravega-data-importer/bin
```
Running the following help commands in above directory show the parameters needed to run the script:
```shell
pravega-data-importer --help
pravega-data-importer stream-mirroring --help
```
This example takes a Pravega Stream running at `localhost:9090` with stream `source/sourceStream` as input, and outputs to
Pravega Stream `destination/destinationStream` running at `localhost:9990`.
```shell
pravega-data-importer
  stream-mirroring
  input-controller=tcp://localhost:9090
  input-stream=source/sourceStream
  input-startAtTail=false
  output-stream=destination/destinationStream
  output-controller=tcp://127.0.0.1:9990
  flinkHost=localhost
  flinkPort=8081
```

To test if the job is working, start up an instance of `pravega-cli` and connect to the destination cluster.
The destination scope and stream should be visible using `scope list` command and `stream list <scope-name>`.

To see the data importer in action, you can use the `consoleWriter` functionality provided in 
[pravega-samples](https://github.com/pravega/pravega-samples) after your Flink job has been submitted. 
After building the project, navigate and run:
```shell
cd pravega-samples/pravega-client-examples/build/install/pravega-client-examples
bin/consoleWriter -scope <source> -name <sourceStream>
```
After entering submitting some input to the `consoleWriter`,
the same output should appear in the destination stream, showing that the job is working.

## Kafka-Stream-Mirroring: Continuously copying a Kafka topic to another Pravega Stream

### Overview

This Flink job will continuously copy a Kafka stream to a Pravega Stream.
It uses Flink checkpoints to provide exactly-once guarantees, ensuring that events
are never missed nor duplicated.
It can use parallelism for high-volume streams with multiple segments.
It marks the associated output Pravega Stream with stream tag `"kafka-mirror"`.

### Run Locally

First build project:
```shell
./gradlew clean build installDist -x test
```
Make sure you have a Flink cluster running locally. In the Flink installation directory, you can run the following
command to start a Flink cluster locally:
```shell
bin/start-cluster.sh
```
In the output Pravega cluster, make sure the associated stream scope has already been created:
In the Pravega CLI:
```shell
scope create <destination-scope>
```
The `help` command in the Pravega CLI gives more information on how to use the Pravega CLI.
Please navigate to this directory once gradle build is complete:
```shell
cd pravega-data-importer/build/install/pravega-data-importer/bin
```
Running the following help commands in above directory show the parameters needed to run the script:
```shell
pravega-data-importer --help
pravega-data-importer kafka-stream-mirroring --help
```
This example takes a Kafka stream running at `localhost:9092` with topic `test-input` and outputs to
Pravega Stream `destination/from-kafka` running at `localhost:9090`.
```shell
pravega-data-importer
  kafka-stream-mirroring
  input-topic=test-input
  bootstrap-servers=localhost:9092
  output-stream=destination/from-kafka
  output-controller=tcp://127.0.0.1:9090
  isStreamOrdered=true
```
To test if the job is working, start up an instance of `pravega-cli` and connect to the destination cluster.
The destination scope and stream should be visible using `scope list` command and `stream list <scope-name>`.

To write to a local Kafka cluster, please install [Kafka](https://kafka.apache.org) and follow the
instructions listed on the [Quickstart](https://kafka.apache.org/quickstart) page.

Once you have Kafka running, running this command will create a `"test-input"` topic with Kafka running at
`localhost:9092`:
```shell
bin/kafka-topics.sh --create --topic test-input --bootstrap-server localhost:9092
```
The data importer can extract lots of different information from a Kafka message, including 
key, value, headers, partition number, topic, timestamp, and timestamp type.

With the following command, key/value pairs can be written:
```shell
bin/kafka-console-producer.sh --topic test-input --bootstrap-server localhost:9092 \
--property parse.key=true --property key.separator=":"
```
On the destination Pravega side, the destination stream should show `PravegaRecord` Java objects with the input,
as well as the other information extracted from Kafka messages mentioned above.