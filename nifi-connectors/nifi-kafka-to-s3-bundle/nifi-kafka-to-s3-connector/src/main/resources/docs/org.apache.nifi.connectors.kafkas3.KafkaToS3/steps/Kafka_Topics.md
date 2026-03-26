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
# Kafka Topics Configuration

This step configures which Kafka topics to consume from and how to consume them.

## Topic Names

Select one or more Kafka topics to consume from. The connector will automatically
fetch the list of available topics from your Kafka cluster.

## Consumer Group ID

Specify a unique consumer group ID for this connector. Kafka uses consumer groups
to track message offsets and ensure each message is processed only once within a group.

**Best Practice:** Use a descriptive name that identifies the purpose of this connector,
such as `kafka-to-s3-production` or `analytics-pipeline-consumer`.

## Offset Reset

Controls the behavior when no prior offset exists or the current offset is invalid:

| Value | Description |
|-------|-------------|
| earliest | Start reading from the oldest available message |
| latest | Start reading from the newest messages only |
| none | Fail if no prior offset exists |

## Kafka Data Format

Specify the format of messages in your Kafka topics:

- **Avro**: Messages are encoded using Apache Avro (requires Schema Registry)
- **JSON**: Messages are plain JSON objects

