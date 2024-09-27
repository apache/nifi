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

# StartSnowflakeIngest

### Description

The StartSnowflakeIngest processor triggers a Snowflake pipe ingestion for a staged file. Please note, that the pipe has
to be created in your Snowflake account manually. The processor requires an upstream connection that provides the path
of the file to be ingested in the stage through the "snowflake.staged.file.path" attribute. This attribute is
automatically filled in by the PutSnowflakeInternalStage processor when using an internal stage. In case a pipe copies
data from an external stage, the attribute shall be manually provided (e.g. with an UpdateAttribute processor). **NOTE:
Since Snowflake pipes ingest files asynchronously, this processor transfers FlowFiles to the "success" relationship when
they're marked for ingestion. In order to wait for the actual result of the ingestion, the processor may be connected to
a downstream GetSnowflakeIngestStatus processor.**

#### Example flow for internal stage

GetFile -> PutSnowflakeInternalStage -> StartSnowflakeIngest -> GetSnowflakeIngestStatus

#### Example flow for external stage

ListS3 -> UpdateAttribute (add the "snowflake.staged.file.path" attribute) -> StartSnowflakeIngest ->
GetSnowflakeIngestStatus