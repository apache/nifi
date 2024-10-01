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

# HandleHttpResponse

## Usage Description:

The pairing of this Processor with a HandleHttpRequest Processor provides the
ability to use NiFi to visually construct a web server that can carry out any functionality that is available through
the existing Processors. For example, one could construct a Web-based front end to an SFTP Server by constructing a flow
such as:

HandleHttpRequest -> PutSFTP -> HandleHttpResponse

This Processor must be configured with the same <HTTP Context Map> service as the corresponding HandleHttpRequest
Processor. Otherwise, all FlowFiles will be routed to the 'failure' relationship.