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

### Group Type

The `Group Type` property selects the Kafka consumer group model.

- `Consumer Group` (the default) uses Kafka's classic consumer-group protocol. Records are bound to partitions: each partition is assigned to one consumer at a time, position is tracked per partition by committed offset, and parallelism (the number of concurrent tasks that can effectively consume) is capped by the number of partitions on the subscribed topics. This is the historical behavior of `ConsumeKafka` and the only option available against Kafka 3.x or 4.0 brokers.
- `Share Group` uses Kafka share groups (KIP-932). Records are distributed cooperatively across the consumers of a share group with per-record acknowledgement instead of per-partition offset commits. Parallelism is no longer capped by the number of partitions, at the cost of weaker per-partition ordering and the loss of the exactly-once handoff pattern with `PublishKafka`.

#### Upgrade compatibility

Existing `ConsumeKafka` flows from earlier NiFi releases are unchanged after upgrading. The new `Group Type` property defaults to `Consumer Group`, so any flow definition that omits the property continues to use the classic consumer-group code path with the same semantics it had before. The classic-group properties (`Topic Format`, `Auto Offset Reset`, `Commit Offsets`) remain at their previously persisted values; they are simply hidden from the UI when `Group Type` is switched to `Share Group`.

#### Share Group prerequisites

Share groups require the following on the broker side:

- All brokers running Apache Kafka **4.2 or later** (recommended). Kafka 4.2 is the first release in which share groups are Generally Available and the version that matches the Kafka client bundled with NiFi.
- Kafka **4.1** brokers are also supported, but share groups are still in preview in that release.
- Kafka 4.0 share-group early-access clients are **not wire-compatible** with 4.1+ brokers; running 4.0 brokers with this processor is not supported.
- The share rebalance protocol enabled in `group.coordinator.rebalance.protocols` (for example `classic,consumer,share`).
- The share coordinator state topic available, with replication and ISR settings appropriate for the cluster (`share.coordinator.state.topic.replication.factor`, `share.coordinator.state.topic.min.isr`).

The Kafka client used by NiFi exposes the share consumer (`org.apache.kafka.clients.consumer.KafkaShareConsumer`) under the `@Evolving` interface stability annotation. APIs and behavior may change in incompatible ways between minor Kafka releases.

#### Setting the starting position for a new share group

Share groups do **not** honor the `auto.offset.reset` consumer property. The starting position for a brand-new share group is governed by the broker-level `share.auto.offset.reset` configuration and is managed out of band via Kafka administrative tooling. Before starting the processor for the first time, set the share group's starting offset using either:

- The `kafka-share-groups.sh --reset-offsets` script that ships with Kafka, or
- The Java Admin API (`Admin.alterShareGroupOffsets`).

If the share group has no recorded starting position when the processor first runs, only records produced after the consumer subscribes are eligible for delivery (the broker default is `latest`). The processor's verification step surfaces a hint in this case.

#### Differences from the Consumer Group path

The following classic-group properties are hidden when `Group Type` is `Share Group`, since they have no analogue:

- `Topic Format` — the Kafka share consumer accepts only an explicit topic-name list. Pattern subscription is not part of the share-consumer API.
- `Auto Offset Reset` — share groups manage starting position at the group level (see above).
- `Commit Offsets` — share groups acknowledge per record rather than committing offsets, so the exactly-once handoff with `PublishKafka` (which depends on deferring offset commits) is not available.
- `Max Uncommitted Size` — the share consumer issues a single `poll()` per processor trigger and acknowledges the full batch atomically, so a size cap cannot be applied incrementally across polls the way it is in classic mode. The size of each batch is therefore controlled at the broker level by `group.share.max.fetch.bytes` rather than from the processor.

Other behavioral differences worth noting:

- A session commit acknowledges all records consumed in the session as `ACCEPT`, including records that were routed to the `parse failure` relationship. To redeliver records on failure, route the `parse failure` relationship to a downstream error-handling subflow rather than relying on broker-side redelivery.
- A session rollback (for example when downstream FlowFile commit fails) releases all records consumed in the session. The release semantics depend on the configured `Acknowledgement Mode` — see below.
- Verifying or sampling a share-group configuration consumes records briefly and releases them back to the share group (`RELEASE` acknowledgement) so they remain available to real consumers, regardless of the configured `Acknowledgement Mode`.

#### Acknowledgement Mode

When `Group Type` is `Share Group`, the `Acknowledgement Mode` property controls how records are acknowledged to the broker:

- `Explicit` (the default) acknowledges every delivered record individually. On a successful session commit each record is acknowledged as `ACCEPT`. On a session rollback the records are acknowledged as `RELEASE` and become **immediately** eligible for redelivery to any consumer in the share group. This is the recommended mode for typical NiFi workloads because it keeps end-to-end redelivery latency low when downstream processing fails.
- `Implicit` lets the broker treat all delivered records as `ACCEPT` on the next poll or commit. Per-record acknowledgement is not permitted in this mode, so a session rollback cannot actively release records back to the broker. Released records only become eligible for redelivery once the broker's record-acquisition lock expires (broker-level `group.share.record.lock.duration.ms`, default `30000`). The processor closes the share consumer on rollback to avoid re-using a session that still has unacknowledged records pending; the next NiFi trigger creates a fresh consumer.

Choose `Explicit` unless you specifically need the broker-side accept-everything behavior of `Implicit` (for example, to trade redelivery latency for slightly lower client overhead in steady-state success cases).

#### Broker-side delivery and retry

Share groups apply a broker-side retry envelope that is independent of any NiFi-side retry policy:

- Each delivery of a record increments a broker-tracked delivery count.
- After `group.share.delivery.attempt.limit` deliveries (broker default `5`, configurable per-group), the broker moves the record to the *archived* state and stops delivering it to any consumer. This is the share-group equivalent of a poison-message dead-letter; archived records are visible via Kafka admin tooling but are no longer delivered.
- Records held by a consumer that does not acknowledge them within `group.share.record.lock.duration.ms` are automatically released by the broker for redelivery, even if the consumer has not invoked `release()` itself. This is also the redelivery mechanism `ConsumeKafka` relies on in `Implicit` mode after a session rollback.

For workloads where NiFi-side error handling is preferred over broker-side retry, route the `parse failure` relationship to a downstream subflow (for example, a `PutKafka`-based dead-letter topic).

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
