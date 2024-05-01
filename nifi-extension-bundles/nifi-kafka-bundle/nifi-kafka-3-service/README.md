<!--
  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.

  The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

  See the License for the specific language governing permissions and limitations under the License.
-->

### Kafka3ProducerService
- As the `send()` API maps cleanly to the context of a single `FlowFile`, additional APIs are useful to handle the ability to publish multiple FlowFiles in the context of a single publish to Kafka.
- Addition of `close()` API to interface allows for graceful handling of `ControllerService` misconfiguration issues (NIFI-12194).
- Addition of `init()`, `complete()` APIs allows for abstraction of transactionality handling, simplifying the `KafkaProducerService` implementation.
