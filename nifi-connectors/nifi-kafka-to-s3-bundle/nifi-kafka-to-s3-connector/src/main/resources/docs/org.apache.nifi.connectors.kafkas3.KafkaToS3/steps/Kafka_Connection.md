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
# Kafka Connection Configuration

This step configures the connection to your Apache Kafka cluster.

## Kafka Server Settings

Enter the bootstrap servers for your Kafka cluster. You can specify multiple brokers
as a comma-separated list (e.g., `broker1:9092,broker2:9092,broker3:9092`).

### Security Configuration

Select the appropriate security protocol based on your Kafka cluster configuration:

| Protocol | Description |
|----------|-------------|
| PLAINTEXT | No encryption or authentication |
| SSL | TLS encryption without SASL authentication |
| SASL_PLAINTEXT | SASL authentication without encryption |
| SASL_SSL | Both SASL authentication and TLS encryption (recommended) |

If using SASL authentication, provide your username and password credentials.

## Schema Registry (Optional)

If your Kafka topics use Avro, Protobuf, or JSON Schema, configure the Schema Registry
URL to enable schema-based serialization and deserialization.

