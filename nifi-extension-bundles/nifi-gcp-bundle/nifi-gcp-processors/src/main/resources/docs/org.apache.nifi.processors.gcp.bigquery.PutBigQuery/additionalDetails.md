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

# PutBigQuery

## Streaming Versus Batching Data

PutBigQuery is record based and is relying on the gRPC based Write API using protocol buffers. The underlying stream
supports both streaming and batching approaches.

### Streaming

With streaming the appended data to the stream is instantly available in BigQuery for reading. It is configurable how
many records (rows) should be appended at once. Only one stream is established per flow file so at the conclusion of the
FlowFile processing the used stream is closed and a new one is opened for the next FlowFile. Supports exactly once
delivery semantics via stream offsets.

### Batching

Similarly to the streaming approach one stream is opened for each FlowFile and records are appended to the stream.
However data is not available in BigQuery until it is committed by the processor at the end of the FlowFile processing.

# Improvement opportunities

* The table has to exist on BigQuery side it is not created automatically
* The Write API supports multiple streams for parallel execution and transactionality across streams. This is not
  utilized at the moment as this would be covered on NiFI framework level.

The official [Google Write API documentation](https://cloud.google.com/bigquery/docs/write-api) provides additional
details.