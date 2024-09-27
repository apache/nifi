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

# GetZendesk

### Description

The processor uses the Zendesk Incremental Exports API to initially export a complete list of items from some arbitrary
milestone, and then periodically poll the API to incrementally export items that have been added or changed since the
previous poll. The processor extracts data from the response and emits a flow file having an array of objects as content
if the response was not empty, also placing an attribute on the flow file having the value of the number of records
fetched. If the response was empty, no flow file is emitted. SplitJson processor can be used the split the array of
records into distinct flow files where each flow file will contain exactly one record.

### Authentication

Zendesk Incremental Exports API uses basic authentication. Either a password or an authentication token have to be
provided. Authentication token can be created in Zendesk API Settings, so the users don't have to expose their
passwords, and also auth tokens can be revoked quickly if necessary.

### Export methods

Zendesk Incremental Exports API supports cursor and time based export methods. Cursor based method is the preferred way
and should be used where available. Due to the limitations of time based export the result set may contain duplicated
records. For more details on export methods please
visit [this guide](https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/)

### Excluding duplicate items

Because of limitations with time-based pagination, the exported data may contain duplicate items. The processor won't do
the deduplication, instead DetectDuplicate or DeduplicateRecord processors can be used with UpdateAttribute processor to
extract the necessary attributes from the flow file content. Please
see [the following guide](https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/#excluding-duplicate-items)
for details and the list of attributes to use in the deduplication process.