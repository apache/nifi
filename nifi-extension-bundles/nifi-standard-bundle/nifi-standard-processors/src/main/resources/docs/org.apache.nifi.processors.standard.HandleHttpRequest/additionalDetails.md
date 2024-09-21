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

# HandleHttpRequest

## Usage Description

The pairing of this Processor with a HandleHttpResponse Processor provides the
ability to use NiFi to visually construct a web server that can carry out any functionality that is available through
the existing Processors. For example, one could construct a Web-based front end to an SFTP Server by constructing a flow
such as:

HandleHttpRequest -> PutSFTP -> HandleHttpResponse

The HandleHttpRequest Processor provides several Properties to configure which methods are supported, the paths that are
supported, and SSL configuration.

To handle requests with Content-Type: _multipart/form-data_ containing multiple parts, additional attention needs to be
paid. Each _part_ generates a FlowFile of its own. To each these FlowFiles, some special attributes are written:

* http.context.identifier
* http.multipart.fragments.sequence.number
* http.multipart.fragments.total.number

These attributes could be used to implement a gating mechanism for HandleHttpResponse processor to wait for the
processing of FlowFiles with sequence number **http.multipart.fragments.sequence.number** until up to *
*http.multipart.fragments.total.number** of flow files are processed, belonging to the same **http.context.identifier**,
which is unique to the request.