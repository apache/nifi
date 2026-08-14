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

# KafkaProvenanceReportingTask

The Kafka Provenance Reporting Task publishes NiFi Provenance Events directly to a Kafka topic using the Kafka
Connection Service. On each scheduling trigger the task reads a configurable batch of events from the provenance
repository and publishes them to Kafka. The ID of the last published event is stored in local state so that the
task resumes from the correct position after a restart.

Events can be filtered by event type, component type, component name, and component ID before publishing.

A configurable message key field controls how Kafka assigns events to partitions, which is useful for grouping
related events (e.g. all events for the same FlowFile or lineage) on the same partition for ordered consumption.

Each event is published as an individual Kafka message. By default events are serialized as JSON objects. When a
Record Writer controller service is configured, the serialization format is delegated to that writer, allowing
formats such as Avro or Parquet. The user can also control which fields are written by defining a schema on the
Record Writer (e.g. a subset of the full reporting task schema), giving full control over the output format and
data. The record schema used as input to the Record Writer is defined as follows:

```json
{
  "type": "record",
  "name": "provenance",
  "namespace": "provenance",
  "fields": [
    {
      "name": "eventId",
      "type": "string"
    },
    {
      "name": "eventOrdinal",
      "type": "long"
    },
    {
      "name": "eventType",
      "type": "string"
    },
    {
      "name": "timestampMillis",
      "type": "long"
    },
    {
      "name": "durationMillis",
      "type": "long"
    },
    {
      "name": "lineageStart",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "details",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "componentId",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "componentType",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "componentName",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "processGroupId",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "processGroupName",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "entityId",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "entityType",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "entitySize",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "previousEntitySize",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "updatedAttributes",
      "type": {
        "type": "map",
        "values": "string"
      }
    },
    {
      "name": "previousAttributes",
      "type": {
        "type": "map",
        "values": "string"
      }
    },
    {
      "name": "actorHostname",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "contentURI",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "previousContentURI",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "parentIds",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "childIds",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "platform",
      "type": "string"
    },
    {
      "name": "application",
      "type": "string"
    },
    {
      "name": "remoteIdentifier",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "alternateIdentifier",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "transitUri",
      "type": [
        "null",
        "string"
      ]
    }
  ]
}
```