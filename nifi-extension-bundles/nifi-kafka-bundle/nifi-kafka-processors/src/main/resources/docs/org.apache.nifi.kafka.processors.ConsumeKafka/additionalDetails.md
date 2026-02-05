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

# ConsumeKafka

### Output Strategies

This processor offers multiple output strategies (configured via processor property 'Output Strategy') for converting Kafka records into FlowFiles.

- 'Use Content as Value' (the default) emits FlowFile records containing only the Kafka record value.
- 'Use Wrapper' emits FlowFile records containing the Kafka record key, value, and headers, as well as additional metadata from the Kafka record.
- 'Inject Metadata' emits FlowFile records containing the Kafka record value into which a sub record field has been added to hold metadata, headers and key information.

The record schema that is used when "Use Wrapper" is selected is as follows (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [{
      "name": "key",
      "type": [{
          < Schema is determined by the Key Record Reader, or will be "string" or "bytes", depending on the "Key Format" property (see below for more details) >
        }, "null"]
    },
    {
      "name": "value",
      "type": [
        {
          < Schema is determined by the Record Reader >
        },
        "null"
      ]
    },
    {
      "name": "headers",
      "type": [
        { "type": "map", "values": "string", "default": {}},
        "null"]
    },
    {
      "name": "metadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "topic", "type": ["string", "null"] },
            { "name": "partition", "type": ["int", "null"] },
            { "name": "offset", "type": ["int", "null"] },
            { "name": "timestamp", "type": ["long", "null"] }
          ]
        },
        "null"
      ]
    }
  ]
}
```

The record schema that is used when "Inject Metadata" is selected is as follows (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
      < Fields as determined by the Record Reader for the Kafka message >,
    {
      "name": "kafkaMetadata",
      "type": [
        {
          "type": "record",
          "name": "metadataType",
          "fields": [
            { "name": "topic", "type": ["string", "null"] },
            { "name": "partition", "type": ["int", "null"] },
            { "name": "offset", "type": ["int", "null"] },
            { "name": "timestamp", "type": ["long", "null"] },
            {
              "name": "headers",
              "type": [ { "type": "map", "values": "string", "default": {}}, "null"]
            },
            {
              "name": "key",
              "type": [{
                < Schema is determined by the Key Record Reader, or will be "string" or "bytes", depending on the "Key Format" property (see below for more details) >
              }, "null"]
            }
          ]
        },
        "null"
      ]
    }
  ]
}
```

If the Output Strategy property is set to 'Use Wrapper' or 'Inject Metadata', an additional processor configuration property ('Key Format') is activated. This property is used to specify how the Kafka Record's key should be written out to the FlowFile. The possible values for 'Key Format' are as follows:

- 'Byte Array' supplies the Kafka Record Key as a byte array, exactly as they are received in the Kafka record.
- 'String' converts the Kafka Record Key bytes into a string using the UTF-8 character encoding. (Failure to parse the key bytes as UTF-8 will result in the record being routed to the 'parse.failure' relationship.)
- 'Record' converts the Kafka Record Key bytes into a deserialized NiFi record, using the associated 'Key Record Reader' controller service. If the Key Format property is set to 'Record', an additional processor configuration property name 'Key Record Reader' is made available. This property is used to specify the Record Reader to use in order to parse the Kafka Record's key as a Record.

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy "Use Wrapper" is selected:

```json
[
  {
    "key": {
      "name": "Acme",
      "number": "AC1234"
    },
    "value": {
      "address": "1234 First Street",
      "zip": "12345",
      "account": {
        "name": "Acme",
        "number": "AC1234"
      }
    },
    "headers": {
      "attributeA": "valueA",
      "attributeB": "valueB"
    },
    "metadata": {
      "topic": "accounts",
      "partition": 0,
      "offset": 0,
      "timestamp": 0
    }
  }
]
```

Here is an example of FlowFile content that is emitted by JsonRecordSetWriter when strategy "Inject Metadata" is selected:

```json
[
  {
    "address": "1234 First Street",
    "zip": "12345",
    "account": {
      "name": "Acme",
      "number": "AC1234"
    },
    "kafkaMetadata": {
      "topic": "accounts",
      "partition": 0,
      "offset": 0,
      "timestamp": 0,
      "headers": {
        "attributeA": "valueA",
        "attributeB": "valueB"
      },
      "key": {
        "name": "Acme",
        "number": "AC1234"
      }
    }
  }
]
```

### Consumer Partition Assignment

By default, this processor will subscribe to one or more Kafka topics in such a way that the topics to consume from are randomly
assigned to the nodes in the NiFi cluster. Consider a scenario where a single Kafka topic has 8 partitions and the consuming
NiFi cluster has 3 nodes. In this scenario, Node 1 may be assigned partitions 0, 1, and 2. Node 2 may be assigned partitions 3, 4, and 5.
Node 3 will then be assigned partitions 6 and 7.

In this scenario, if Node 3 somehow fails or stops pulling data from Kafka, partitions 6 and 7 may then be reassigned to the other two nodes.
For most use cases, this is desirable. It provides fault tolerance and allows the remaining nodes to pick up the slack. However, there are cases
where this is undesirable.

One such case is when using NiFi to consume Change Data Capture (CDC) data from Kafka. Consider again the above scenario. Consider that Node 3
has pulled 1,000 messages from Kafka but has not yet delivered them to their final destination. NiFi is then stopped and restarted, and that takes
15 minutes to complete. In the meantime, Partitions 6 and 7 have been reassigned to the other nodes. Those nodes then proceeded to pull data from
Kafka and deliver it to the desired destination. After 15 minutes, Node 3 rejoins the cluster and then continues to deliver its 1,000 messages that
it has already pulled from Kafka to the destination system. Now, those records have been delivered out of order.

The solution for this, then, is to assign partitions statically instead of dynamically. In this way, we can assign Partitions 6 and 7 to Node 3 specifically.
Then, if Node 3 is restarted, the other nodes will not pull data from Partitions 6 and 7. The data will remain queued in Kafka until Node 3 is restarted. By
using this approach, we can ensure that the data that already was pulled can be processed (assuming First In First Out Prioritizers are used) before newer messages
are handled.

In order to provide a static mapping of node to Kafka partition(s), one or more user-defined properties must be added using the naming scheme
<code>partitions.&lt;hostname&gt;</code> with the value being a comma-separated list of Kafka partitions to use. For example,
<code>partitions.nifi-01=0, 3, 6, 9</code>, <code>partitions.nifi-02=1, 4, 7, 10</code>, and <code>partitions.nifi-03=2, 5, 8, 11</code>.
The hostname that is used can be the fully qualified hostname, the "simple" hostname, or the IP address. There must be an entry for each node in
the cluster, or the Processor will become invalid. If it is desirable for a node to not have any partitions assigned to it, a Property may be
added for the hostname with an empty string as the value.

NiFi cannot readily validate that all Partitions have been assigned before the Processor is scheduled to run. However, it can validate that no
partitions have been skipped. As such, if partitions 0, 1, and 3 are assigned but not partition 2, the Processor will not be valid. However,
if partitions 0, 1, and 2 are assigned, the Processor will become valid, even if there are 4 partitions on the Topic. When the Processor is
started, the Processor will immediately start to fail, logging errors, and avoid pulling any data until the Processor is updated to account
for all partitions. Once running, if the number of partitions is changed, the Processor will continue to run but not pull data from the newly
added partitions. Once stopped, it will begin to error until all partitions have been assigned. Additionally, if partitions that are assigned
do not exist (e.g., partitions 0, 1, 2, 3, 4, 5, 6, and 7 are assigned, but the Topic has only 4 partitions), then the Processor will begin
to log errors on startup and will not pull data.

In order to use a static mapping of Kafka partitions, the "Topic Name Format" must be set to "names" rather than "pattern." Additionally, all
Topics that are to be consumed must have the same number of partitions. If multiple Topics are to be consumed and have a different number of
partitions, multiple Processors must be used so that each Processor consumes only from Topics with the same number of partitions.
