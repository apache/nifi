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

# GetSnowflakeIngestStatus

### Description

The GetSnowflakeIngestStatus processor can be used to get the status of a staged file ingested by a Snowflake pipe. To
wait until a staged file is fully ingested (copied into the table) you should connect this processor's "retry"
relationship to itself. The processor requires an upstream connection that provides the path of the staged file to be
checked through the "snowflake.staged.file.path" attribute. See StartSnowflakeIngest processor for details about how to
properly set up a flow to ingest staged files. **NOTE: Snowflake pipes cache the paths of ingested files and never
ingest the same file multiple times. This can cause the processor to enter an "infinite loop" with a FlowFile that has
the same "snowflake.staged.file.path" attribute as a staged file that has been previously ingested by the pipe. It is
recommended that the retry mechanism be configured to avoid these scenarios.**