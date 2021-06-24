<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Introduction

The Apache NiFi application can be thought of as two separate but intertwined components: the flow authorship component
and the flow engine. By bringing these two components together into a single application, NiFi allows users to
author a dataflow and run it in real-time in the same user interface.

However, these two concepts can be separated. NiFi can be used to author flows, which can then be run by not only
NiFi but also other compatible dataflow engines. The Apache NiFi project provides several of these dataflow engines:
Apache NiFi itself, MiNiFi Java (A sub-project of Apache NiFi), MiNiFi C++ (A sub-project of Apache NiFi), and
Stateless NiFi.

Each of these dataflow engines has its own sets of strengths and weaknesses and as a result have their own particular
use cases that they solve best. This document will describe what Stateless NiFi is, how to use it, and its strengths
and weaknesses.



# Traditional NiFi

NiFi is designed to be run as a large, multi-tenant application. It strives to take full advantage of all resources
given to it, to include disks/storage and many threads. Typically, a single NiFi instance is clustered across many
different nodes to form a large, cohesive dataflow, which may be made up of many different sub-flows. NiFi, in general,
will assume ownership of data that is delivered to it. It stores that data reliably on disk until it has been delivered
to all necessary destinations. Delivery of this data may be prioritized at different points in the flow so that data
that is most important to a particular destination gets delivered to that destination first, while that same data may
be delivered to another destination in a different order based on prioritization. NiFi does all of this while maintaining
very fine-grained lineage and holding a buffer of data as it was seen by every component in the flow (the combination of
the data lineage and the rolling buffer of data is referred to as Data Provenance).

Each of these features is important to provide a very powerful, broad, holistic view of how data is operated on, and flows
through, an enterprise. There are use cases, however, that would be better served by a much lighter weight application.
An application that is capable of interacting with all of the different endpoints that NiFi can interact with and perform
all of the transformations, routing, filtering, and processing that NiFi can perform. But an application that is designed
to run only a small sub-flow, not a large dataflow with many sources and sinks.


# Stateless NiFi 

Enter Stateless NiFi (also referred to in this document as simply "Stateless").

Many of the concepts in Stateless NiFi differ from those in the typical Apache NiFi engine.

Stateless provides a dataflow engine with a smaller footprint. It does not include a user interface for
authoring or monitoring dataflows but instead runs dataflows that were authored using the NiFi application.
While NiFi performs best when it has access to fast storage such as SSD and NVMe drives, Stateless stores
all data in memory.

This means that if Stateless NiFi is stopped, it will no longer have direct access to the data that was in-flight.
As a result, Stateless should only be used for dataflows where the data source is both reliable and replayable, or
in scenarios where data loss is not a critical concern.

A very common use case is to have Stateless NiFi read data from Apache Kafka or JMS and then perform some routing/filtering/
manipulation and finally deliver the data to another destination. If a dataflow like this were to be run within NiFi,
the data would be consumed from the source, written to NiFi's internal repositories, and acknowledged, so that NiFi will have
taken ownership of that data. It will then be responsible for delivering it to all destinations, even if the application
is restarted.

With Stateless NiFi, though, the data would be consumed and then transferred to the next processor in the flow. The data
would not be written to any sort of internal repository, and it would not yet be acknowledged. The next processor in the
flow would process the data, and then pass it along. Only once the data reaches the end of the entire dataflow would the
data received from the source be acknowledged. If Stateless is restarted before the processing completes, the data has
not yet been acknowledged, so it is simply consumed again. This allows the data to be processed in-memory without fear
of data loss, but it does also put onus on the source to store the data reliably and make the data replayable.


## Compatible Dataflows

As mentioned above, Stateless NiFi requires that the source of data be both reliable and replayable. This limits
the sources that Stateless can reasonably interact with. Additionally, there are a few other limitations to
the dataflows that the Stateless engine is capable of running.

#### Single Source, Single Destination

Each dataflow that is run in Stateless should be kept to a single source and a single sink, or destination.
Because Stateless does not store data that it is processing, and does not store metadata such as where data is
queued up in a dataflow, sending a single FlowFile to multiple destinations can result in data duplication.

Consider a flow where data is consumed from Apache Kafka and then delivered to both HDFS and S3. If data is stored
in HDFS, and then storing to S3 fails, the entire session will be rolled back, and the data will have to be consumed
again. As a result, the data may be consumed and delivered to HDFS a second time. If this continues to happen, the data
will be continually fetched from Kafka and stored in HDFS. Depending on the destination and the flow configuration, this
may not be a concern (aside from wasted resources) but in many cases, this is a significant concern.

Therefore, if the dataflow is to be run with the Stateless engine, a dataflow such a this should be broken apart into two
different dataflows. The first would deliver data from Apache Kafka to HDFS and the other would deliver data from Apache Kafka
to S3. Each of these dataflows should then use a separate Consumer Group for Kafka, which will result in each dataflow getting
a copy of the same data.

#### Support for Merging May Be Limited

Because data in Stateless NiFi transits through the dataflow synchronously from start to finish, use of Processors
that require multiple FlowFiles, such as MergeContent and MergeRecord, may not be capable of receiving all of the data that is
necessary in order to succeed. If a Processor has data queued up and is triggered, but fails to make any progress, the Stateless
Engine will trigger the source processor again in order to provide additional data to the Processor.

However, this can lead to a situation in which data is continually brought in, depending on how the Processor behaves. To avoid this,
the amount of data that may be brought into a single invocation of the dataflow may be limited via configuration. If the dataflow configuration
limits the amount of data per invocation to say 10 MB, but MergeContent is configured not to create a bin until at least 100 MB of data is
available, the dataflow will continue to trigger MergeContent to run, without making any progress, until either the Max Bin Age is reached
(if configured) or the dataflow times out.

Additionally, depending on the context in which Stateless is run, triggering the source components may not provide additional data.
For example, if Stateless is run in an enviornment where data is queued up in an Input Port and then the dataflow is triggered, subsequently
triggering the Input Port to run will not produce additional data.

As a result, it is important to ensure that any dataflows that contain logic to merge FlowFiles is configured with a Max Bin Age for MergeContent
and MergeRecord.

#### Failure Handling

In traditional NiFi, it is common to loop a 'failure' connection from a given Processor back to the same Processor.
This results in the Processor continually trying to process the FlowFile until it is successful. This can be extremely important,
because typically one NiFi receives the data, it is responsible for taking ownership of that data and must be able to hold the data
until the downstream services is able to receive it and then delivery that data.

With Stateless NiFi, however, the source of the data is assumed to be both reliable and replayable. Additionally, Stateless NiFi,
by design, will not hold data after restarts. As a result, the failure handling considerations may be different. With Stateless NiFi,
if unable to deliver data to the downstream system, it is often preferable to instead route the FlowFile to an Output Port and then
mark that Output Port as a failure port (see [Failure Ports](#failure-ports) below for more information).


#### Flows Should Not Load Massive Files

In traditional NiFi, FlowFile content is stored on disk, not in memmory. As a result, it is capable of handling any size
data as long as it fits on the disk. However, in Stateless, FlowFile contents are stored in memory, in the JVM heap. As
a result, it is generally not advisable to attempt to load massive files, such as a 100 GB dataset, into Stateless NiFi.
Doing so will often result in an OutOfMemoryError, or at a minimum cause significant garbage collection, which can degrade
performance.



## Feature Comparisons

As mentioned above, Stateless NiFi offers a different set of features and tradeoffs from traditional NiFi.
Here, we summarize the key differences. This comparison is not exhaustive but provides a quick look at how
the two runtimes operate.

| Feature | Traditional NiFi | Stateless NiFi |
|---------|------------------|----------------|
| Data Durability | Data is reliably stored on disk in the FlowFile and Content Repositories | Data is stored in-memory and must be consumed from the source again upon restart |
| Data Ordering | Data is ordered independently in each Connection based on the selected Prioritizers | Data flows through the system in the order it was received (First-In, First-Out / FIFO) |
| Site-to-Site | Supports full Site-to-Site capabilities, including Server and Client roles | Can push to, or pull from, a NiFi instance but cannot receive incoming Site-to-Site connections. I.e., works as a client but not a server. |
| Form Factor | Large form factor. Designed to take advantage of many cores and disks. | Light-weight form factor. Easily embedded into another application. Single-threaded processing. |
| Heap Considerations | Typically, many processors in use by many users. FlowFile content should not be loaded into heap because it can easily cause heap exhaustion. | Smaller dataflows use less heap. Flow operates on only one or a few FlowFiles at a time and holds FlowFile contents in memory in the Java heap. |
| Data Provenance | Fully stored, indexed data provenance that can be browsed through the UI and exported via Reporting Tasks | Limited Data Provenance capabilities, events being stored in memory. No ability to view but can be exported using Reporting Tasks. However, since they are in-memory, they will be lost upon restart and may roll off before they can be exported. |
| Embeddability | While technically possible to embed traditional NiFi, it is not recommended, as it launches a heavy-weight User Interface, deals with complex authentication and authorization, and several file-based external dependencies, which can be difficult to manage. | Has minimal external dependencies (directory containing extensions and a working directory to use for temporary storage) and is much simpler to manage. Embeddability is an important feature of Stateless NiFi. |
 
## Running Stateless NiFi

Stateless NiFi can be used as a library and embedded into other applications. However, it can also be run directly
from the command-line from a NiFi build using the `bin/nifi.sh` script.

To do so requires three files:

- The engine configuration properties file
- The dataflow configuration properties file
- The dataflow itself (which may exist as a file, or point to a flow in a NiFi registry)

Stateless NiFi accepts two separate configuration files: an engine configuration file and a dataflow configuration file.
This is done because typically the engine configuration will be the same for all flows that are run, so it can be created
only once. The dataflow configuration will be different for each dataflow that is to be run.

An example of running stateless NiFi:

```
bin/nifi.sh stateless -c -e /var/lib/nifi/stateless/config/stateless.properties -f /var/lib/nifi/stateless/flows/jms-to-kafka.properties
```

Here, the `-c` option indicates that the flow should be continually triggered, not just triggered once.
The last two sets of arguments provide the properties file for the stateless engine and the properties file for the dataflow, respectively.


#### Engine Configuration

All properties in the Engine Configuration file are prefixed with `nifi.stateless.`. Below is a list of property names,
descriptions, and example values:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.nar.directory | The location of a directory containing all NiFi Archives (NARs) that are necessary for running the dataflow | /var/lib/nifi/lib |
| nifi.stateless.working.directory | The location of a directory where Stateless should store its expanded NAR files and use for temporary storage | /var/lib/nifi/work/stateless |


The following properties may be used for configuring security parameters:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.security.truststore | Filename of a Truststore to use for Site-to-Site or for interacting with NiFi Registry or Extension Clients | /etc/certs/truststore.jks |
| nifi.stateless.security.truststoreType | The type of the Truststore such as PKCS12 | JKS |
| nifi.stateless.security.truststorePasswd | The password of the Truststore. | do-not-use-this-password |
| nifi.stateless.security.keystore | Filename of a Keystore to use for Site-to-Site or for interacting with NiFi Registry or Extension Clients | /etc/certs/keystore.jks |
| nifi.stateless.security.keystoreType | The type of the Keystore such as PKCS12 | JKS |
| nifi.stateless.security.keystorePasswd | The password of the Keystore | do-not-use-this-password-either |
| nifi.stateless.security.keyPasswd | An optional password for the key in the Keystore. If not specified, the password of the Keystore itself will be used. | password |
| nifi.stateless.sensitive.props.key | The dataflow does not hold sensitive passwords, but some processors may have a need to encrypt data before storing it. This key is used to allow processors to encrypt and decrypt data. At present, the only Processor supported by the community that makes use of this feature is hte GetJMSTopic processor, which is deprecated. However, it is provided here for completeness. | Some Passphrase That's Difficult to Guess |
| nifi.stateless.kerberos.krb5.file | The KRB5 file to use for interacting with Kerberos. This is only necessary if the dataflow interacts with a Kerberized data source/sink. If not specified, will default to `/etc/krb5.conf` | /etc/krb5.conf |


When Stateless NiFi is started, it parses the provided dataflow and determines which bundles/extensions are necessary
to run the dataflow. If an extension is not available, or the version referenced by the flow is not available, Stateless
may attempt to download the extensions automatically. To do this, one or more Extension Clients must be configured.
Each client is configured using several properties, which are all tied together using a 'key'. For example, if we have
4 properties, `nifi.stateless.extension.client.ABC.type`, `nifi.stateless.extension.client.ABC.baseUrl`,
`nifi.stateless.extension.client.XYZ.type`, and `nifi.stateless.extension.client.XYZ.baseUrl`, then we know that
the first `type` property refers to the same client as the first `baseUrl` property because they both have the 'key'
`ABC`. Similarly, the second `type` and `baseUrl` properties refer to the same client because they have the same 'key':
`XYZ`.

Any extension that is downloaded will be stored in the directory specified by the `nifi.stateless.nar.directory` property described above.

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.extension.client.\<key>.type | The type of Extension Client. Currently, the only supported value is 'nexus'. | nexus |
| nifi.stateless.extension.client.\<key>.baseUrl | The Base URL to use when connecting to the service. The example here is for Maven Central. | https://repo1.maven.org/maven2/ |
| nifi.stateless.extension.client.\<key>.timeout | The amount of time to wait to connect to the system or receive data from the system. | 30 secs |
| nifi.stateless.extension.client.\<key>.useSslContext | If the Base URL indicates that the HTTPS protocol is to be used, this property dictates whether the SSL Context defined above is to be used or not. If not, then the default Java truststore information will be used. | false |

A full example of the Engine Configuration may look as follows:

```
nifi.stateless.nar.directory=/var/lib/nifi/lib
nifi.stateless.working.directory=/var/lib/nifi/work/stateless

nifi.stateless.security.keystore=/etc/certs/keystore.jks
nifi.stateless.security.keystoreType=JKS
nifi.stateless.security.keystorePasswd=my-keystore-password
nifi.stateless.security.keyPasswd=
nifi.stateless.security.truststore=/etc/certs/truststore.jks
nifi.stateless.security.truststoreType=JKS
nifi.stateless.security.truststorePasswd=my-truststore-password
nifi.stateless.sensitive.props.key=nifi-stateless

# Pull extensions from Maven Central
nifi.stateless.extension.client.mvn-central.type=nexus
nifi.stateless.extension.client.mvn-central.timeout=30 sec
nifi.stateless.extension.client.mvn-central.baseUrl=https://repo1.maven.org/maven2/
nifi.stateless.extension.client.mvn-central.useSslContext=false

nifi.stateless.kerberos.krb5.file=/etc/krb5.conf
```


A minimum configuration of the Engine Configuration may look as follows:
```
nifi.stateless.nar.directory=/var/lib/nifi/lib
nifi.stateless.working.directory=/var/lib/nifi/work/stateless
```


#### Dataflow Configuration

While the Engine Configuration above gives Stateless NiFi the necessary information for how to run the flow, the dataflow
configuration provides it with the necessary information for what flow to run.

The flow's location must be provided either by specifying a NiFi Registry URL, Bucket ID, and Flow ID (and optional version);
by specifying a local filename for the flow; by specifying a URL for the flow; or by including a "stringified" version of the JSON flow definition itself.
Note that if using a local filename, the format of the file is not the same as
the `flow.xml.gz` file that NiFi uses but rather is the `Versioned Flow Snapshot` format that is used by the NiFi Registry.
The easiest way to export a flow from NiFi onto local disk for use by Stateless NiFi is to right-click on a Process Group or
the canvas in NiFi and choose `Downlaod Flow`.

The following properties are supported for specifying the location of a flow:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.registry.url | The URL of the NiFi Registry to source the dataflow from. If specified, the `flow.bucketId` and the `flow.id` must also be specified. | https://nifi-registry/ |
| nifi.stateless.flow.bucketId | The UUID of the bucket in NiFi Registry that holds the flow. | 00000000-0000-0000-0000-000000000011 |
| nifi.stateless.flow.id | The UUID of the flow in NiFi Registry. | 00000000-0000-0000-0000-000000000044 |
| nifi.stateless.flow.version | The version of the dataflow to run. If not specified, will use the latest version of the flow. | 5 |
| nifi.stateless.flow.snapshot.file | Instead of using the NiFi Registry to source the flow, the flow can be a local file. In this case, this provides the filename of the file. | /var/lib/nifi/flows/my-flow.json |
| nifi.stateless.flow.snapshot.url | A URL that contains the Flow Definition to use. | https://gist.github.com/apache/223389cb6cbbd82985fbb8d429b58899 |
| nifi.stateless.flow.snapshot.url.use.ssl.context | A boolean value indicating whether or not the SSL Context that is defined in the Engine Configuration properties file should be used when downloading the flow | false | 

Stateless NiFi also allows the user to provide one or more Parameter Contexts to use in the dataflow:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.parameters.\<key> | The name of the Parameter Context. This must match the name of a Parameter Context that is referenced within the dataflow. | My Parameter Context |
| nifi.stateless.parameters.\<key>.\<parameter name> | The name of a Parameter to use, the value of the property being the value of the Parameter | My Value |

For example, to create a Parameter Context with the name "Kafka Parameter Context" and 2 parameters, "Kafka Topic" and "Kafka Brokers",
we would use the following three properties:
```
nifi.stateless.parameters.kafka=Kafa Parameter Context
nifi.stateless.parameters.kafka.Kafka Topic=Sensor Data
nifi.stateless.parameters.kafka.Kafka Brokers=kafka-01:9092,kafka-02:9092,kafka-03:9092
```

Note that while Java properties files typically do not allow for spaces in property names, Stateless parses the properties
files in a way that does allow for spaces, so that Parameter names, etc. may allow for spaces.

There are times, however, when we do not want to provide the list of Parameters in the dataflow properties file. We may want to fetch the Parameters from some file or
an external service. For this reason, Stateless supports a notion of a Parameter Provider. A Parameter Provider is an extension point that can be used to retrieve Parameters
from elsewhere. For information on how to configure Parameter Provider, see the [Passing Parameters](#passing-parameters) section below.

When a stateless dataflow is triggered, it can also be important to consider how much data should be allowed to enter the dataflow for a given invocation.
Typically, this consists of a single FlowFile at a time or a single batch of FlowFiles at a time, depending on the source processor. However, some processors may
require additional data in order to perform their tasks. For example, if we have a dataflow whose source processor brings in a single message from a JMS Queue, and
later in the flow have a MergeContent processor, that MergeContent processor may not be able to perform its function with just one message. As a result, the source
processor will be triggered again. This process will continue until either the MergeContent processor is able to make progress and empty its incoming FlowFile Queues
OR until some threshold has been reached. These thresholds can be configured using the following properties:


/ Property Name / Description / Example Value /
/---------------/-------------/---------------/
/ nifi.stateless.transaction.thresholds.flowfiles / The maximum number of FlowFiles that a source processors should bring into the flow each time the dataflow is triggered. / 1000 /
/ nifi.stateless.transaction.thresholds.bytes / The maximum amount of data for all FlowFiles' contents. / 100 MB /
/ nifi.stateless.transaction.thresholds.time / The amount of time between when the dataflow was triggered and when the source processors should stop being triggered. / 1 sec /

For example, to ensure that the source processors are not triggered to bring in more than 1 MB of data and not more than 10 FlowFiles, we can use:
```
nifi.stateless.transaction.thresholds.flowfiles=10
nifi.stateless.transaction.thresholds.bytes=1 MB
```  

With this configuration, each time the dataflow is triggered, the source processor (or all sources, cumulatively, if there is more than one) will not be triggered again after it has brought
10 FlowFiles OR 1 MB worth of FlowFile content (regardless if that 1 MB was from 1 FlowFiles or the sum of all FlowFiles) into the flow.
Note, however, that if the source were to bring in 1,000 FlowFiles and 50 MB of data in a single invocation, that would be allowed, but the component would no longer be triggered until the dataflow
has completed.


##### Reporting Tasks

The dataflow configuration also allows for defining Reporting Tasks. Similarly, multiple properties for a given Reporting Task
are tied together with a common key. The following properties are supported:


| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.reporting.task.\<key>.name | The name of the Reporting Task | Log Status
| nifi.stateless.reporting.task.\<key>.type | The type of the Reporting Task. This may be the fully qualified classname or the simple name, if only a single class exists with the simple name | ControllerStatusReportingTask |
| nifi.stateless.reporting.task.\<key>.bundle | The bundle that holds the Reporting Task. If not specified, the bundle will be automatically identified, if there exists exactly one bundle with the reporting task. However, if no Bundle is specified, none will be downloaded and if more than 1 is already available, the Reporting Task cannot be created. The format is \<group id>:\<artifact id>:\<version> | org.apache.nifi:nifi-standard-nar:1.12.1 |
| nifi.stateless.reporting.task.\<key>.properties.\<property name> | One or more Reporting Task properties may be configured using this syntax | Any valid value for the corresponding property |
| nifi.stateless.reporting.task.\<key>.frequency | How often the Reporting Task should be triggered | 2 sec |

An example Reporting Task that will log stats to the log file every 30 seconds is as follows:

```
nifi.stateless.reporting.task.stats.name=Stats
nifi.stateless.reporting.task.stats.type=ControllerStatusReportingTask
nifi.stateless.reporting.task.stats.bundle=org.apache.nifi:nifi-standard-nar:1.12.1
nifi.stateless.reporting.task.stats.properties.Show Deltas=false
nifi.stateless.reporting.task.stats.properties.Reporting Granularity=One Second  # Log 1-second metrics instead of 5-minute metrics
nifi.stateless.reporting.task.stats.frequency=30 sec
```

##### Failure Ports

There is one additional property that is supported in the dataflow configuration:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.failure.port.names | A comma-delimited list of Output Port names. If a FlowFile is routed to any of these Output Ports, it is considered a failure and will rollback the entire session. | Unknown Kafka Type, Parse Failure, Failed to Write to HDFS |

This property allows the user to enter one or more ports that should be considered failures. The value is a comma-separted list of names of Outport Ports. In the example above, if a FlowFile is
routed to the "Unknown Kafka Type" port, the "Parse Failure" port, or the "Failed to Write to HDFS" port, then the flow is considered a failure. The entire session will be rolled back, and the source
Processor will not acknowledge the data from the source. As a result, the next time that the dataflow is triggered, it will consume the same data again.

While used as an illustrative example here, it may not make sense to route data to a "Parse Failure" Output Port and consider that a failure, though. That's because a Parse Failure is likely not
going to succeed the next time around. Such a case may result in constantly consuming the same data and attempting to process it over and over again. However, it may make sense if the use case
dictates that no more data may be processed until such a message on the Kafka queue has been properly dealt with. 

##### Full Examples

An example of a fully formed dataflow configuration file that will import a dataflow from NiFi Registry is as follows:
```
nifi.stateless.registry.url=https://nifi-registry/
nifi.stateless.flow.bucketId=00000000-0000-0000-0000-000000000011
nifi.stateless.flow.id=00000000-0000-0000-0000-000000000044
nifi.stateless.flow.version=5

nifi.stateless.parameters.kafkahdfs=Kafka to HDFS
nifi.stateless.parameters.kafkahdfs.Kafka Topic=Sensor Data
nifi.stateless.parameters.kafkahdfs.Kafka Brokers=kafka-01:9092,kafka-02:9092,kafka-03:9093
nifi.stateless.parameters.kafkahdfs.HDFS Directory=/data/sensors

nifi.stateless.reporting.task.stats.name=Stats
nifi.stateless.reporting.task.stats.type=ControllerStatusReportingTask
nifi.stateless.reporting.task.stats.bundle=org.apache.nifi:nifi-standard-nar:1.12.1
nifi.stateless.reporting.task.stats.properties.Show Deltas=false
nifi.stateless.reporting.task.stats.frequency=1 minute
nifi.stateless.reporting.task.stats.properties.Reporting Granularity=One Second
```

An alternate example, referencing a locally stored JSON file for the dataflow:
```
nifi.stateless.flow.snapshot.file=/var/lib/nifi/stateless-flows/kafka-to-hdfs.json

nifi.stateless.parameters.kafkahdfs=Kafka to HDFS
nifi.stateless.parameters.kafkahdfs.Kafka Topic=Sensor Data
nifi.stateless.parameters.kafkahdfs.Kafka Brokers=kafka-01:9092,kafka-02:9092,kafka-03:9093
nifi.stateless.parameters.kafkahdfs.HDFS Directory=/data/sensors

nifi.stateless.reporting.task.stats.name=Stats
nifi.stateless.reporting.task.stats.type=ControllerStatusReportingTask
nifi.stateless.reporting.task.stats.bundle=org.apache.nifi:nifi-standard-nar:1.12.1
nifi.stateless.reporting.task.stats.properties.Show Deltas=false
nifi.stateless.reporting.task.stats.frequency=1 minute
nifi.stateless.reporting.task.stats.properties.Reporting Granularity=One Second
```


An example of a minimal configuration with no Reporting Tasks or Parameters:
```
nifi.stateless.flow.snapshot.file=/var/lib/nifi/stateless-flows/kafka-to-hdfs.json
```


#### Passing Parameters

There are times when it is not convenient to pass Parameters in the .properties file.
For example, the same dataflow may be reused for several different sources or sinks, and each time the flow is run,
it needs to be run with a different set of Parameters.

Additionally, there may be sensitive parameters that users prefer not to include in the .properties file. These may be provided via
Environment Variables, for example.

These parameters may be passed when running NiFi via the `bin/nifi.sh` script by passing a `-p` argument.
When used, the `-p` argument must be followed by an argument in the format `[<context name>:]<parameter name>=<parameter value>`
For example:

```
bin/nifi.sh stateless -c -p "Kafka Parameter Context:Kafka Topic=Sensor Data" /var/lib/nifi/stateless/config/stateless.properties /var/lib/nifi/stateless/flows/jms-to-kafka.properties
```

Note that because of the spaces in the Parameter/Context name and the Parameter value, the argument is quoted.
Multiple Parameters may be passed using this syntax:

```
bin/nifi.sh stateless -c -p "Kafka Parameter Context:Kafka Brokers=kafka-01:9092,kafka-02:9092,kafka-03:9092" -p "Kafka Parameter Context:Kafka Topic=Sensor Data" /var/lib/nifi/stateless/config /stateless.properties
```

If the name of the Parameter Context contains a colon, it must be escaped using a backslash.
The name of the Parameter Context and the name of the Parameter may not include an equals sign (=).
The Parameter Value can include colon characters, as well as equals, as in the example here.

Often times, though, the Parameter Context name is not particularly important, and we just want to provide a Parameter name.
This can be done by simply leaving off the name of the Parameter Context. For example:

```
bin/nifi.sh stateless -c -p "Kafka Brokers=kafka-01:9092,kafka-02:9092,kafka-03:9092" -p "Kafka Topic=Sensor Data" /var/lib/nifi/stateless/config /stateless.properties
```

In this case, any Parameter Context that has a name of "Kafka Brokers" will have the parameter resolved to `kafka-01:9092,kafka-02:9092,kafka-03:9092`, regardless of the name
of the Parameter Context.

If a given Parameter is referenced and is not defined using the `-p` syntax, an environment variable may also be used to provide the value. However, environment variables typically are
allowed to contain only letters, numbers, and underscores in their names. As a result, it is important that the Parameters' names also adhere to that same rule, or the environment variable
will not be addressable.

At times, none of the built-in capabilities for resolving Parameters are ideal, though. In these situations, we can use a custom Parameter Provider in order to source Parameter values from elsewhere.
To configure a custom Parameter Provider, we must configure it similarly to Reporting Tasks, using a common key to indicate which Parameter Provider the property belongs to.
The following properties are supported:

| Property Name | Description | Example Value |
|---------------|-------------|---------------|
| nifi.stateless.parameter.provider.\<key>.name | The name of the Parameter Provider | My Secret Parameter Provider
| nifi.stateless.parameter.provider.\<key>.type | The type of the Parameter Provider. This may be the fully qualified classname or the simple name, if only a single class exists with the simple name | MySecretParameterProvider |
| nifi.stateless.parameter.provider.\<key>.bundle | The bundle that holds the Parameter Provider. If not specified, the bundle will be automatically identified, if there exists exactly one bundle with the reporting task. However, if no Bundle is specified, none will be downloaded and if more than 1 is already available, the Parameter Provider cannot be created. The format is \<group id>:\<artifact id>:\<version> | org.apache.nifi:nifi-standard-nar:1.14.0 |
| nifi.stateless.parameter.provider.\<key>.properties.\<property name> | One or more Parameter Provider properties may be configured using this syntax | Any valid value for the corresponding property |

An example Parameter Provider might be configured as follows:

```
nifi.stateless.parameter.provider.Props File Provider.name=My Custom Properties File Parameter Provider
nifi.stateless.parameter.provider.Props File Provider.type=com.myorg.nifi.parameters.custom.MyCustomPropertiesFileParameterProvider
nifi.stateless.parameter.provider.Props File Provider.bundle=com.myorg:nifi-custom-parameter-provider-nar:0.0.1
nifi.stateless.parameter.provider.Props File Provider.properties.Filename=/tmp/parameters.properties
```
