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

Apache NiFi is a very powerful tool for authoring and running dataflows. It provides many capabilities that are necessary for large-scale
enterprise deployments, such as data persistence and resilience, data lineage and traceability, and multi-tenancy. This, however, requires
that an administrator be responsible for ensuring that this process is running and operational. And generally, adding more capabilities
results in more complexity.

There are times, however, when users don't need all of the power of NiFi and would like to run in a much simpler form factor. A common use case
is to use NiFi to pull data from many different sources, perform manipulations (e.g., convert JSON to Avro), filter some records, and then to publish
the data to Apache Kafka. Another common use case is to pull data from Apache Kafka, perform some manipulations and filtering, and then publish the
data elsewhere.

For deployments where NiFi acts only as a bridge into and out of Kafka, it may be simpler to operationalize such a deployment by having the dataflow
run within Kafka Connect.

The NiFi Kafka Connector allows users to do just that!


# Stateless NiFi

When a dataflow is to be run within Kafka Connect, it is run using the Stateless NiFi dataflow engine. For more information, see the README of the
Stateless NiFi module.

Stateless NiFi differs from the traditional NiFi engine in a few ways. For one, Stateless NiFi is an engine that is designed to be embedded. This makes it
very convenient to run within the Kafka Connect framework.

Stateless NiFi does not provide a user interface (UI) or a REST API and does not support modifying the dataflow while it is running.

Stateless NiFi does not persist FlowFile content to disk but rather holds the content in memory. Stateless NiFi operates on data in a First-In-First-Out order,
rather than using data prioritizers. Dataflows built for Stateless NiFi should have a single source and a single destination (or multiple destinations, if 
data is expected to be routed to exactly one of them, such as a 'Failure' destination and a 'Success' destination).

Stateless NiFi does not currently provide access to data lineage/provenance.

Stateless NiFi does not support cyclic graphs. While it is common and desirable in traditional NiFi to have a 'failure' relationship from a Processor
route back to the same processor, this can result in a StackOverflowException in Stateless NiFi. The preferred approach in Stateless NiFi is to create
an Output Port for failures and route the data to that Output Port. 

# Configuring Stateless NiFi Connectors

NiFi supports two different Kafka Connectors: a source connector and a sink connector. Each of these have much of the same configuration
elements but do have some differences. The configuration for each of these connectors is described below.

In order to build a flow that will be run in Kafka Connect, the dataflow must first be built. This is accomplished using a traditional deployment
of NiFi. So, while NiFi does not have to be deployed in a production environment in order to use the Kafka Connector, it is necessary in a development
environment for building the actual dataflow that is to be deployed.

To build a flow for running in Kafka Connect, the flow should be built within a Process Group. Once read, the Process Group can be exported by right-clicking
on the Process Group and clicking "Download flow" or by saving the flow to a NiFi Registry instance.


## Source Connector

The NiFi Source Connector is responsible for obtaining data from one source and delivering that data to Kafka. The dataflow should not attempt to
deliver directly to Kafka itself via a Processor such as PublishKafka. Instead, the data should be routed to an Output Port. Any FlowFile delivered
to that Output Port will be obtained by the connector and delivered to Kafka. It is important to note that each FlowFile is delivered as a single Kafka
record. Therefore, if a FlowFile contains thousands of JSON records totaling 5 MB, for instance, it will be important to split those FlowFiles up into
individual Records using a SplitRecord processor before transferring the data to the Output Port. Otherwise, this would result in a single Kafka record
that is 5 MB.

In order to deploy the Source Connector, the nifi-kafka-connector-<version>-bin.tar.gz must first be unpacked into the directory where Kafka Connect
is configured to load connectors from. For example:

```
cd /opt/kafka/kafka-connectors
tar xzvf ~/Downloads/nifi-kafka-connector-1.13.0-bin.tar.gz
```

At this point, if Kafka Connect is already running, it must be restarted in order to pick up the new Connector. This connector supplies both the
NiFi Source Connector and the NiFi Sink Connector.

Next, we must create a configuration JSON that tells Kafka Connect how to deploy an instance of the Connector. This is the standard Kafka Connect
JSON configuration. However, it does require a few different configuration elements specific to the dataflow. Let us consider a dataflow that listens
for Syslog events, converts them into JSON, and then delivers the JSON to Kafka. The dataflow would consist of a single ListenSyslog Processor, with
the "success" relationship going to an Output Port with name `Syslog Messages`.

After creating this simple dataflow, we must place the dataflow in a location where the Kafka Connect connector is able to retrieve it. We could simply
copy the file to each Connect node. Or, we could host the dataflow somewhere that it can be pulled by each Connect instance. To download the flow, right-click
on the Process Group containing our dataflow and choose "Download flow" from the menu. From there, we can upload the JSON to Github as a Gist, for example.


### Configuring Source Connector

An example configuration JSON to run this dataflow would then look like this (note that this cannot be copied and pasted as JSON as is,
as it includes annotations (1), (2), etc. for illustrative purposes):

```
{
  "name": "syslog-to-kafka",
  "config": {
(1)    "connector.class": "org.apache.nifi.kafka.connect.StatelessNiFiSourceConnector",
(2)    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
(3)    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
(4)    "tasks.max": "1",
(5)    "name": "syslog-to-kafka",
(6)    "working.directory": "./working/stateless",
(7)    "topics": "syslog-gateway-json",
(8)    "nexus.url": "https://repo1.maven.org/maven2/",
(9)    "flow.snapshot": "https://gist.githubusercontent.com/user/6123f4b890f402c0b512888ccc92537e/raw/5b41a9eb6db09c54a4d325fec78a6e19c9abf9f2/syslog-to-kafka.json",
(10)   "key.attribute": "syslog.hostname",
(11)   "output.port": "Syslog Messages",
(12)   "header.attribute.regex": "syslog.*",
(13)   "krb5.file": "/etc/krb5.conf",
(14)   "dataflow.timeout": "30 sec",
(15)   "parameter.Syslog Port": "19944",
(16)   "extensions.directory": "/tmp/stateless-extensions"
  }
}
``` 

The first two elements, `name` and `config` are standard for all Kafka Connect deployments. Within the `config` element are several different fields that
will be explained here.

`(1) connector.class`: This is the name of the class to use for the Connector. The given value indicates that we want to use Stateless NiFi as a source for
our data. If the desire was instead to publish data from Kafka to another destination, we would use the value `org.apache.nifi.kafka.connect.StatelessNiFiSinkConnector`.

`(2) key.converter`: This specifies how to interpret the Kafka key. When the `key.attribute` field is specified, as in `(10)` above, this tells us that we
want to use whatever value is in the attribute name as the Kafka message key. In this example, the value of the "syslog.hostname" FlowFile attribute in NiFi
will be used as the Kafka message key.

`(3) value.converter`: Specifies how to convert the payload of the NiFi FlowFile into bytes that can be written to Kafka. Generally, with Stateless NiFi, it is
recommended to use the `value.converter` of `org.apache.kafka.connect.converters.ByteArrayConverter` as NiFi already has the data serialized as a byte array
and is very adept at formatting the data as it needs to be.

`(4) tasks.max`: The maximum number of tasks/threads to use to run the dataflow. Unlike traditional NiFi, with Stateless NiFi, the entire dataflow is run from start
to finish with a single thread. However, multiple threads can be used to run multiple copies of the dataflow.

`(5) name`: The name of the connect instance. This should match the first `name` field.

`(6) working.directory`: Optional. Specifies a directory on the Connect server that NiFi should use for unpacking extensions that it needs to perform the dataflow.
If not specified, defaults to `/tmp/nifi-stateless-working`.

`(7) topics`: The name of the topic to deliver data to. All FlowFiles will be delivered to the topic whose name is specified here. However, it may be
advantageous to determine the topic individually for each FlowFile. To achieve this, simply ensure that the dataflow specifies the topic name in an attribute,
and then use `topic.name.attribute` to specify the name of the attribute instead of `topic.name`. For example, if we wanted a separate Kafka topic for each
Syslog sender, we could omit `topic.name` and instead provide `"topic.name.attribute": "syslog.hostname"`.

`(8) nexus.url`: Traditional NiFi is deployed with many different extensions. In addition to that, many other third party extensions have been developed but
are not included in the distribution due to size constraints. It is important that the NiFi Kafka Connector not attempt to bundle all possible extensions. As
a result, Connect can be configured with the URL of a Nexus server. The example above points to Maven Central, which holds the released versions of the NiFi
extensions. When a connector is started, it will first identify which extensions are necessary to run the dataflow, determine which extensions are available,
and then automatically download any necessary extensions that it currently does not have available. If configuring a Nexus instance that has multiple repositories,
the name of the repository should be included in the URL. For example: `https://nexus-private.myorganization.org/nexus/repository/my-repository/`.

`(9) flow.snapshot`: Specifies the dataflow to run. This is the file that was downloaded by right-clicking on the Process Group in NiFi and
clicking "Download flow". The dataflow can be stored external to the configured and the location can be represented as an HTTP (or HTTPS URL), or a filename.
If specifying a filename, that file must be present on all Kafka Connect nodes. Because of that, it may be simpler to host the dataflow somewhere.
Alternatively, the contents of the dataflow may be "Stringified" and included directly as the value for this property. This can be done, for example,
using the `jq` tool, such as `jq -R -s '.' <dataflow_file.json>` and the output can then be included in the Kafka Connect configuration JSON.
The process of escaping the JSON and including it within the Kafka Connect configuration may be less desirable if building the configuration manually,
but it can be beneficial if deploying from an automated system. It is important to note that if using a file or URL to specify the dataflow, it is important
that the contents of that file/URL not be overwritten in order to change the dataflow. Doing so can result in different Kafka Connect tasks running different
versions of the dataflow. Instead, a new file/URL should be created for the new version, and the Kafka Connect task should be updated to point to the new version.
This will allow Kafka Connect to properly update all tasks.

`(10) key.attribute`: Optional. Specifies the name of a FlowFile attribute that should be used to specify the key of the Kafka record. If not specified, the Kafka record
will not have a key associated with it. If specified, but the attribute does not exist on a particular FlowFile, it will also have no key associated with it.

`(11) output.port`: Optional. The name of the Output Port in the NiFi dataflow that FlowFiles should be pulled from. If the dataflow contains exactly one port, this property
is optional and can be omitted. However, if the dataflow contains multiple ports (for example, a 'Success' and a 'Failure' port), this property must be specified.
If any FlowFile is sent to any port other than the specified Port, it is considered a failure. The session is rolled back and no data is collected.

`(12) header.name.regex`: Optional. A Java Regular Expression that will be evaluated against all FlowFile attribute names. For any attribute whose name matches the Regular Expression,
the Kafka record will have a header whose name matches the attribute name and whose value matches the attribute value. If not specified, the Kafka record will have no
headers added to it.

`(13) krb5.conf`: Optional. Specifies the krb5.conf file to use if the dataflow interacts with any services that are secured via Kerberos. If not specified, will default
to `/etc/krb5.conf`.

`(14) dataflow.timeout`: Optional. Specifies the maximum amount of time to wait for the dataflow to complete. If the dataflow does not complete before this timeout,
the thread will be interrupted, and the dataflow is considered to have failed. The session will be rolled back and the connector will trigger the flow again. If not
specified, defaults to `30 secs`.

`(15) parameter.XYZ`: Optional. Specifies a Parameter to use in the Dataflow. In this case, the JSON field name is `parameter.Syslog Port`. Therefore, any Parameter Context
in the dataflow that has a parameter with name `Syslog Port` will get the value specified (i.e., `19944` in this case). If the dataflow has child Process Groups, and those child
Process Groups have their own Parameter Contexts, this value will be used for any and all Parameter Contexts containing a Parameter by the name of `Syslog Port`. If the Parameter
should be applied only to a specific Parameter Context, the name of the Parameter Context may be supplied and separated from the Parameter Name with a colon. For example,
`parameter.Syslog Context:Syslog Port`. In this case, the only Parameter Context whose `Syslog Port` parameter would be set would be the Parameter Context whose name is `Syslog Context`.

`(16) extensions.directory` : Specifies the directory to add any downloaded extensions to. If not specified, the extensions will be written to the same directory that the
connector lives in. Because this directory may not be writable, and to aid in upgrade scenarios, it is highly recommended that this property be configured.
 

### Transactional sources

Unlike with traditional NiFi, Stateless NiFi keeps the contents of FlowFiles in memory. It is important to understand that as long as the source of the data is
replayable and transactional, there is no concern over data loss. This is handled by treating the entire dataflow as a single transaction. Once data is obtained
from some source component, the data is transferred to the next processor in the flow. At that point, in traditional NiFi, the processor would acknowledge the data
and NiFi would take ownership of that data, having persisted it to disk.

With Stateless NiFi, however, the data will not yet be acknowledged. Instead, the data will be transferred to the next processor in the flow, and it will perform
its task. The data is then transferred to the next processor in the flow and it will complete its task. This is repeated until all data is queued up at the Output Port.
At that point, the NiFi connector will provide the data to Kafka. Only after Kafka has acknowledged the records does the connector acknowledge this to NiFi. And only
at that point is the session committed, allowing the source processor to acknowledge receipt of the data.

As a result, if NiFi is restarted while processing some piece of data, the source processor will not have acknowledged the data and as a result is able to replay the
data, resulting in no data loss. 



## Sink Connector

The NiFi Sink Connector is responsible for obtaining data from Kafka and delivering data to some other service. The dataflow should not attempt to
source data directly from Kafka itself via a Processor such as ConsumeKafka. Instead, the data should be received from an Input Port. Each Kafka record
will be enqueued into the outbound connection of that Input Port so that the next processor in the flow is able to process it.
It is important to note that each Kafka record FlowFile is delivered as a single FlowFile. Depending on the destination, it may be advantageous to merge
many of these Kafka records together before delivering them. For example, if delivering to HDFS, we certainly do not want to send each individual Kafka
message to HDFS as a separate file. For more information and restrictions on merging data within Stateless NiFi, see the [Merging](#merging) section below.

In order to deploy a connector to Kafka Connect, we must create a configuration JSON that tells Kafka Connect how to deploy an instance of the Connector.
This is the standard Kafka Connect JSON configuration. However, it does require a few different configuration elements specific to the dataflow. Let us consider a dataflow that receives
events from Kafka and delivers them to HDFS. The dataflow would consist of a single PutHDFS Processor, with
the "success" relationship going to an Output Port with name `Success` and the "failure" relationship going to an Output Port with name `Failure`.
The PutHDFS processor would be fed data from an Input Port with name `Input`.

After creating this simple dataflow, we must place the dataflow in a location where the Kafka Connect connector is able to retrieve it. We could simply
copy the file to each Connect node. Or, we could host the dataflow somewhere that it can be pulled by each Connect instance. To do this, right-click
on the Process Group containing our dataflow and choose Download flow. From there, we can upload the JSON to Github as a Gist, for example.


### Configuring Sink Connector

An example configuration JSON to run this dataflow would then look like this (note that this cannot be copied and pasted as JSON as is,
as it includes annotations (1), (2), etc. for illustrative purposes):

```
{
  "name": "kafka-to-hdfs",
  "config": {
(1)    "connector.class": "org.apache.nifi.kafka.connect.StatelessNiFiSinkConnector",
(2)    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
(3)    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
(4)    "tasks.max": "1",
(5)    "name": "kafka-to-hdfs",
(6)    "working.directory": "./working/stateless",
(7)    "topics": "syslog-gateway-json",
(8)    "nexus.url": "https://repo1.maven.org/maven2/",
(9)    "flow.snapshot": "https://gist.githubusercontent.com/user/6123f4b890f402c0b512888ccc92537e/raw/5b41a9eb6db09c54a4d325fec78a6e19c9abf9f2/kafka-to-hdfs.json",
(10)   "input.port": "Syslog Messages",
(11)   "failure.ports": "",
(12)   "headers.as.attributes.regex": "syslog.*",
(13)   "krb5.file": "/etc/krb5.conf",
(14)   "dataflow.timeout": "30 sec",
(15)   "parameter.Directory": "/syslog",
(16)   "extensions.directory": "/tmp/stateless-extensions"
  }
}
``` 

The first two elements, `name` and `config` are standard for all Kafka Connect deployments. Within the `config` element are several different fields that
will be explained here.

`(1) connector.class`: This is the name of the class to use for the Connector. The given value indicates that we want to use Stateless NiFi as a source for
our data. If the desire was instead to publish data from Kafka to another destination, we would use the value `org.apache.nifi.kafka.connect.StatelessNiFiSinkConnector`.

`(2) key.converter`: This specifies how to interpret the Kafka key. When the `key.attribute` field is specified, as in `(10)` above, this tells us that we
want to use whatever value is in the attribute name as the Kafka message key. In this example, the value of the "syslog.hostname" FlowFile attribute in NiFi
will be used as the Kafka message key.

`(3) value.converter`: Specifies how to convert the payload of the NiFi FlowFile into bytes that can be written to Kafka. Generally, with Stateless NiFi, it is
recommended to use the `value.converter` of `org.apache.kafka.connect.converters.ByteArrayConverter` as NiFi already has the data serialized as a byte array
and is very adept at formatting the data as it needs to be.

`(4) tasks.max`: The maximum number of tasks/threads to use to run the dataflow. Unlike traditional NiFi, with Stateless NiFi, the entire dataflow is run from start
to finish with a single thread. However, multiple threads can be used to run multiple copies of the dataflow.

`(5) name`: The name of the connect instance. This should match the first `name` field.

`(6) working.directory`: Optional. Specifies a directory on the Connect server that NiFi should use for unpacking extensions that it needs to perform the dataflow.
If not specified, defaults to `/tmp/nifi-stateless-working`.

`(7) topics`: A comma-separated list of Kafka topics to source data from.

`(8) nexus.url`: Traditional NiFi is deployed with many different extensions. In addition to that, many other third party extensions have been developed but
are not included in the distribution due to size constraints. It is important that the NiFi Kafka Connector not attempt to bundle all possible extensions. As
a result, Connect can be configured with the URL of a Nexus server. The example above points to Maven Central, which holds the released versions of the NiFi
extensions. When a connector is started, it will first identify which extensions are necessary to run the dataflow, determine which extensions are available,
and then automatically download any necessary extensions that it currently does not have available. If configuring a Nexus instance that has multiple repositories,
the name of the repository should be included in the URL. For example: `https://nexus-private.myorganization.org/nexus/repository/my-repository/`.

`(9) flow.snapshot`: Specifies the dataflow to run. This is the file that was downloaded by right-clicking on the Process Group in NiFi and
clicking "Download flow". The dataflow can be stored external to the configured and the location can be represented as an HTTP (or HTTPS URL), or a filename.
If specifying a filename, that file must be present on all Kafka Connect nodes. Because of that, it may be simpler to host the dataflow somewhere.
Alternatively, the contents of the dataflow may be "Stringified" and included directly as the value for this property. This can be done, for example,
using the `jq` tool, such as `cat <dataflow_file.json> | jq -R -s '.'` and the output can then be included in the Kafka Connect configuration JSON.
The process of escaping the JSON and including it within the Kafka Connect configuration may be less desirable if building the configuration manually,
but it can be beneficial if deploying from an automated system. It is important to note that if using a file or URL to specify the dataflow, it is important
that the contents of that file/URL not be overwritten in order to change the dataflow. Doing so can result in different Kafka Connect tasks running different
versions of the dataflow. Instead, a new file/URL should be created for the new version, and the Kafka Connect task should be updated to point to the new version.
This will allow Kafka Connect to properly update all tasks.

`(10) input.port`: Optional. The name of the Input Port in the NiFi dataflow that Kafka records should be sent to. If the dataflow contains exactly one Input Port,
this property is optional and can be omitted. However, if the dataflow contains multiple Input Ports, this property must be specified.

`(11) failure.ports`: Optional. A comma-separated list of Output Ports that should be considered failure conditions. If any FlowFile is routed to an Output Port,
and the name of that Output Port is provided in this list, the dataflow is considered a failure and the session is rolled back. The dataflow will then wait a bit
and attempt to process the Kafka records again. If data is transferred to an Output Port that is not in the list of failure ports, the data will simply be discarded.

`(12) headers.as.attributes.regex`: Optional. A Java Regular Expression that will be evaluated against all Kafka record headers. For any header whose key matches the
Regular Expression, the header will be added to the FlowFile as an attribute. The attribute name will be the same as the header key, and the attribute value will be
the same as the header value.

`(13) krb5.conf`: Optional. Specifies the krb5.conf file to use if the dataflow interacts with any services that are secured via Kerberos. If not specified, will default
to `/etc/krb5.conf`.

`(14) dataflow.timeout`: Optional. Specifies the maximum amount of time to wait for the dataflow to complete. If the dataflow does not complete before this timeout,
the thread will be interrupted, and the dataflow is considered to have failed. The session will be rolled back and the connector will trigger the flow again. If not
specified, defaults to `30 secs`.

`(15) parameter.XYZ`: Optional. Specifies a Parameter to use in the Dataflow. In this case, the JSON field name is `parameter.Directory`. Therefore, any Parameter Context
in the dataflow that has a parameter with name `Directory` will get the value specified (i.e., `/syslog` in this case). If the dataflow has child Process Groups, and those child
Process Groups have their own Parameter Contexts, this value will be used for any and all Parameter Contexts containing a Parameter by the name of `Directory`. If the Parameter
should be applied only to a specific Parameter Context, the name of the Parameter Context may be supplied and separated from the Parameter Name with a colon. For example,
`parameter.HDFS:Directory`. In this case, the only Parameter Context whose `Directory` parameter would be set would be the Parameter Context whose name is `HDFS`.

`(16) extensions.directory` : Specifies the directory to add any downloaded extensions to. If not specified, the extensions will be written to the same directory that the
connector lives in. Because this directory may not be writable, and to aid in upgrade scenarios, it is highly recommended that this property be configured.


<a name="merging"></a>
### Merging

NiFi supports many different Processors that can be used as sinks for Apache Kafka data. For services that play well in the world of streaming, these
can often be delivered directly to a sink. For example, a PublishJMS processor is happy to receive many small messages. However, if the data is to be
sent to S3 or to HDFS, those services will perform much better if the data is first batched, or merged, together. The MergeContent and MergeRecord
processors are extremely popular in NiFi for this reason. They allow many small FlowFiles to be merged together into one larger FlowFile.

With traditional NiFi, we can simply set a minimum and maximum size for the merged data along with a timeout. However, with Stateless NiFi and Kafka Connect,
this may not work as well, because only a limited number of FlowFiles will be made available to the Processor. We can still use these Processor in
order to merge the data together, but with a bit of a limitation.

If MergeContent / MergeRecord are triggered but do not have enough FlowFiles to create a batch, the processor will do nothing. If there are more FlowFiles
queued up than the configured maximum number of entries, the Processor will merge up to that number of FlowFiles but then leave the rest sitting in the queue.
The next invocation will then not have enough FlowFiles to create a batch and therefore will remain queued. In either of these situations, the result can be
that the dataflow is constantly triggering the merge processor, which makes no process, and as a result the dataflow times out and rolls back the entire session.
Therefore, it is advisable that the MergeContent / MergeRecord processors be configured with a `Minimum Number of Entries` of `1` and a very large value for the
`Maximum Number of Entries` property (for example 1000000). Kafka Connect properties such as `offset.flush.timeout.ms` may be used to control
the amount of data that gets merged together.



# Installing the NiFi Connector

Now that we have covered how to build a dataflow that can be used as a Kafka Connector, and we've discussed how to build the configuration for that connector,
all we have left is describe how we can deploy the Connector itself.

In order to deploy the NiFi Connector, the nifi-kafka-connector-<version>-bin.tar.gz must first be unpacked into the directory where Kafka Connect
is configured to load connectors from (this is configured in the Kafka Connect properties file). For example:

```
cd /opt/kafka/kafka-connectors
tar xzvf ~/Downloads/nifi-kafka-connector-1.13.0-bin.tar.gz
```

At this point, if Kafka Connect is already running, it must be restarted in order to pick up the new Connector. It is not necessary to copy the connector
and restart Kafka Connect each time we want to deploy a dataflow as a connector - only when installing the NiFi connector initially. 
This packaged connector supplies both the NiFi Source Connector and the NiFi Sink Connector.


# Deploying a NiFi Dataflow as a Connector

Once the NiFi connector is installed, we are ready to deploy our dataflow! Depending on the dataflow, we may need a source or a sink connector. Please see
the above sections on creating the appropriate configuration for the connector of interest.

Assuming that we have created the appropriate JSON configuration file for our connector and named it `syslog-to-kafka.json`, we can deploy the flow.
We do this by making a RESTful POST call to Kafka Connect:

```
connect-configurations $ curl -X POST kafka-01:8083/connectors -H "Content-Type: application/json" -d @syslog-to-kafka.json
```

This should produce a response similar to this (formatted for readability):
```
{
  "name": "syslog-to-kafka",
  "config": {
    "connector.class": "org.apache.nifi.kafka.connect.StatelessNiFiSourceConnector",
    "tasks.max": "1",
    "working.directory": "./working/stateless",
    "name": "syslog-to-kafka",
    "topic.name": "syslog-gateway-json",
    "parameter.Syslog Port": "19944"
    "nexus.url": "https://repo1.maven.org/maven2/",
    "flow.snapshot": "https://gist.githubusercontent.com/user/6123f4b890f402c0b512888ccc92537e/raw/5b41a9eb6db09c54a4d325fec78a6e19c9abf9f2/syslog-to-kafka.json",
  },
  "tasks": [],
  "type": "source"
}
``` 

At this point, the connector has been deployed, and we can see in the Kafka Connect logs that the connector was successfully deployed.
The logs will contain many initialization messages, but should contain a message such as:

```
[2020-12-15 10:44:04,175] INFO NiFi Stateless Engine and Dataflow created and initialized in 2146 millis (org.apache.nifi.stateless.flow.StandardStatelessDataflowFactory:202)d
```

This indicates that the engine has successfully parsed the dataflow and started the appropriate components.
Depending on the hardware, this may take only a few milliseconds or it may take a few seconds. However, this assumes that the Connector
already has access to all of the NiFi extensions that it needs. If that is not the case, startup may take longer as it downloads the extensions
that are necessary. This is described in the next section.

Similarly, we can view which connectors are deployed:

```
connect-configurations $ curl kafka-01:8083/connectors
```

Which should produce an output such as:
```
["syslog-to-kafka"]
```

We can then terminate our deployment:

```
curl -X DELETE kafka-01:8083/connectors/syslog-to-kafka
```

# Sourcing Extensions

The list of NiFi extensions that are available numbers into the hundreds, with more being developed by the NiFi community continually.
It would not be ideal to package together all NiFi extensions in to the Connector. Therefore, the NiFi Kafka Connector includes no extensions
at all. This results in the connector occupying a measly 27 MB at the time of this writing.

Obviously, though, the extensions must be downloaded at some point. When a connector is deployed and started up, one of the first things that the
Connector does is to obtain the dataflow configuration, and then extract the information about which extensions are necessary for running the dataflow.
The Connector will then examine its own set of downloaded extensions and determine which ones it is missing. It will then create a List of necessary extensions
and begin downloading them.

In order to do this, the connect configuration must specify where to download the extensions. This is the reason for the "nexus.url" property that is described
in both the Source Connector and the Sink Connector. Once downloaded, the extensions are placed in the configured extensions directory (configured via the
`extensions.directory` configuration element).
If the `extensions.directory` is not explicitly specified in the connector configuration, extensions will be added to the NAR Directory 
(configured via the `nar.directory` configuration element). If this is not specified, it is is auto-detected to be the same directory
that the NiFi Kafka Connector was installed in.


# Mapping of NiFi Features

There are some features that exist in NiFi that have very nice corollaries in Kafka Connect. These are discussed here.

#### State Management

In NiFi, a Processor is capable of storing state about the work that it has accomplished. This is particularly important for source
components such as ListS3. This Processor keeps state about the data that it has already seen so that it does not constantly list the same files repeatedly.
When using the Stateless NiFi Source Connector, the state that is stored by these processors is provided to Kafka Connect and is stored within Kafka itself
as the "Source Offsets" and "Source Partition" information. This allows a task to be restarted and resume where it left off. If a Processor stores
"Local State" in NiFi, it will be stored in Kafka using a "Source Partition" that corresponds directly to that task. As a result, each Task is analogous
to a Node in a NiFi cluster. If the Processor stores "Cluster-Wide State" in NiFi, the state will be stored in Kafka using a "Source Partition" that corresponds
to a cluster-wide state.

Kafka Connect does not allow for state to be stored for Sink Tasks.

#### Primary Node

NiFi provides several processors that are expected to run only on a single node in the cluster. This is accomplished by setting the Execution Node to
"Primary Node Only" in the scheduling tab when configuring a NiFi Processor. When using the Source Connector, if any source processor in the configured
dataflow is set to run on Primary Node Only, only a single task will ever run, even if the "tasks" configuration element is set to a large value. In this
case, a warning will be logged if attempting to use multiple tasks for a dataflow that has a source processor configured for Primary Node Only. Because Processors
should only be scheduled on Primary Node Only if they are sources of data, this is ignored for all Sink Tasks and for any Processor in a Source Task that has
incoming connections.

#### Processor Yielding

When a Processor determines that it is not capable of performing any work (for example, because the system that the Processor is pulling from has no more data to pull),
it may choose to yield. This means that the Processor will not run for some amount of time. The amount of time can be configured in a NiFi dataflow by
configuring the Processor and going to the Settings tab and updating the "Yield Duration" property. When using the Source Connector, if a Processor chooses
to yield, the Source Connector will pause for the configured amount of time before triggering the dataflow to run again.

