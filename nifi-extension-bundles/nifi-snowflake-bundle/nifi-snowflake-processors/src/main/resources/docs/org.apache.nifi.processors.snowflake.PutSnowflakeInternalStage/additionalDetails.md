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

# PutSnowflakeInternalStage

### Description

The PutSnowflakeInternalStage processor can upload a file to a Snowflake internal stage. Please note, that named stages
needs to be created in your Snowflake account manually. See the documentation on how to set up an internal
stage [here](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html). The processor
requires an upstream connection and the incoming FlowFiles' content will be uploaded to the stage. A unique file name is
generated for the file's staged file name. While the processor may be used separately, it's recommended to connect it to
a StartSnowflakeIngest processor so that the uploaded file can be piped into your Snowflake table.