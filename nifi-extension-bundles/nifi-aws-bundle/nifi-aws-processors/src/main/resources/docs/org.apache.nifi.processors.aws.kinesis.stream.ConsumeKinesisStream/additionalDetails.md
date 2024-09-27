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

# ConsumeKinesisStream

## Streaming Versus Batch Processing

ConsumeKinesisStream retrieves all Kinesis Records that it encounters in the configured Kinesis Stream. There are two
common, broadly defined use cases.

### Per-Message Use Case

By default, the Processor will create a separate FlowFile for each Kinesis Record (message) in the Stream and add
attributes for shard id, sequence number, etc.

### Per-Batch Use Case

Another common use case is the desire to process all Kinesis Records retrieved from the Stream in a batch as a single
FlowFile.

The ConsumeKinesisStream Processor can optionally be configured with a Record Reader and Record Writer. When a Record
Reader and Record Writer are configured, a single FlowFile will be created that will contain a Record for each Record
within the batch of Kinesis Records (messages), instead of a separate FlowFile per Kinesis Record.

The FlowFiles emitted in this mode will include the standard `record.*` attributes along with the same Kinesis Shard ID,
Sequence Number and Approximate Arrival Timestamp; but the values will relate to the **last** Kinesis Record that was
processed in the batch of messages constituting the content of the FlowFile.