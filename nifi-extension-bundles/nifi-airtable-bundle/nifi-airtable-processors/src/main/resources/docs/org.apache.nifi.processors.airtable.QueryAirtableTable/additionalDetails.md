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

# QueryAirtableTable

### Description

Airtable is a spreadsheet-database hybrid. In Airtable an application is called base and each base can have multiple
tables. A table consists of records (rows) and each record can have multiple fields (columns). The QueryAirtableTable
processor can query records from a single base and table via Airtable's REST API. The processor utilizes streams to be
able to handle a large number of records. It can also split large record sets to multiple FlowFiles just like a database
processor.

### Personal Access Token

Please note that API Keys were deprecated, Airtable now provides Personal Access Tokens (PATs) instead.
Airtable REST API calls requires a PAT (Personal Access Token) that needs to be passed in a request. An Airtable account
is required to generate the PAT.

### API rate limit

The Airtable REST API limits the number of requests that can be sent on a per-base basis to avoid bottlenecks.
Currently, this limit is 5 requests per second per base. If this limit is exceeded you can't make another request for 30
seconds. It's your responsibility to handle this rate limit via configuring Yield Duration and Run Schedule properly. It
is recommended to start off with the default settings and to increase both parameters when rate limit issues occur.

### Metadata API

Currently, the Metadata API of Airtable is unstable, and we don't provide a way to use it. Until it becomes stable you
can set up a ConvertRecord or MergeRecord processor with a JsonTreeReader to read the content and convert it into a
Record with schema.