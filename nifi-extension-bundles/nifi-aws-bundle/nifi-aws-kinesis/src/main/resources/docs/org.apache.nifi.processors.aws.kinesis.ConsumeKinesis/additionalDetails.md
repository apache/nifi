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

# ConsumeKinesis

## Record processing

When _Processing Strategy_ property is set to _RECORD_, _ConsumeKinesis_ operates in Record mode.
In this mode, the processor reads records from Kinesis streams using the configured _Record Reader_,
and writes them to FlowFiles using the configured _Record Writer_.

The processor tries to optimize the number of FlowFiles created by batching multiple records with the same schema
into a single FlowFile.

### Schema changes

_ConsumeKinesis_ supports dynamically changing Kinesis record schemas. When a record with new schema is encountered,
the currently open FlowFile is closed and a new FlowFile is created for the new schema. Thanks to this, the processor
preserves record ordering for a single Shard, even with record schema changes.

**Please note**, the processor relies on _Record Reader_ to provide correct schema for each record.
Using a Reader with schema inference may produce a lot of different schemas, which may lead to excessive FlowFile creation.
If performance is a concern, it is recommended to use a Reader with a predefined schema or schema registry.
