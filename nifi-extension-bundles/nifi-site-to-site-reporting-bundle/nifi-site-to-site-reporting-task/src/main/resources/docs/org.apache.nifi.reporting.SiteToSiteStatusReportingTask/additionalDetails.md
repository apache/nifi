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

# SiteToSiteStatusReportingTask

The Site-to-Site Status Reporting Task allows the user to publish Status events using the Site To Site protocol. The
component type and name filter regexes form a union: only components matching both regexes will be reported. However,
all process groups are recursively searched for matching components, regardless of whether the process group matches the
component filters.

## Record writer

The user can define a Record Writer and directly specify the output format and data with the assumption that the input
schema is the following:

```json
{
  "type": "record",
  "name": "status",
  "namespace": "status",
  "fields": [
    {
      "name": "statusId",
      "type": "string"
    },
    {
      "name": "timestampMillis",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "timestamp",
      "type": "string"
    },
    {
      "name": "actorHostname",
      "type": "string"
    },
    {
      "name": "componentType",
      "type": "string"
    },
    {
      "name": "componentName",
      "type": "string"
    },
    {
      "name": "parentId",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "parentName",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "parentPath",
      "type": [
        "string",
        "null"
      ]
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
      "name": "componentId",
      "type": "string"
    },
    {
      "name": "activeThreadCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "flowFilesReceived",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "flowFilesSent",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "bytesReceived",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "bytesSent",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "queuedCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "bytesRead",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "bytesWritten",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "terminatedThreadCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "runStatus",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "bytesTransferred",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "flowFilesTransferred",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "inputContentSize",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "outputContentSize",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "queuedContentSize",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "versionedFlowState",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "activeRemotePortCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "inactiveRemotePortCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "receivedContentSize",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "receivedCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "sentContentSize",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "sentCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "averageLineageDuration",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "transmissionStatus",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "targetURI",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "inputBytes",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "inputCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "outputBytes",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "outputCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "transmitting",
      "type": [
        "boolean",
        "null"
      ]
    },
    {
      "name": "sourceId",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "sourceName",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "destinationId",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "destinationName",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "maxQueuedBytes",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "maxQueuedCount",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "queuedBytes",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "backPressureBytesThreshold",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "backPressureObjectThreshold",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "backPressureDataSizeThreshold",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "isBackPressureEnabled",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "processorType",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "averageLineageDurationMS",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "flowFilesRemoved",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "invocations",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "processingNanos",
      "type": [
        "long",
        "null"
      ]
    },
    {
      "name": "executionNode",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "counters",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ]
    }
  ]
}
```