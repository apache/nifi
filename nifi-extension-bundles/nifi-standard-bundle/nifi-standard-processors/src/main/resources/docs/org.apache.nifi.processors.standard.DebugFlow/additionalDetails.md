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

# DebugFlow

When triggered, the processor loops through the appropriate response list. A response is produced the configured number
of times for each pass through its response list, as long as the processor is running.

Triggered by a FlowFile, the processor can produce the following responses.

1. transfer FlowFile to success relationship.
2. transfer FlowFile to failure relationship.
3. rollback the FlowFile without penalty.
4. rollback the FlowFile and yield the context.
5. rollback the FlowFile with penalty.
6. throw an exception.

Triggered without a FlowFile, the processor can produce the following responses.

1. do nothing and return.
2. throw an exception.
3. yield the context.