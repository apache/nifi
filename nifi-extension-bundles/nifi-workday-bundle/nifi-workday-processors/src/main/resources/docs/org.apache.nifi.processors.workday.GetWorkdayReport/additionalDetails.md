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

# GetWorkdayReport

## Summary

This processor acts as a client endpoint to interact with the Workday API. It is capable of reading reports from Workday
RaaS and transferring the content directly to the output, or you can define the required Record Reader and RecordSet
Writer, so you can transform the report to the required format.

## Supported report formats

* csv
* simplexml
* json

In case of json source you need to set the following parameters in the JsonTreeReader:

* Starting Field Strategy: Nested Field
* Starting Field Name: Report\_Entry

It is possible to hide specific columns from the response if you define the Writer scheme explicitly in the
configuration of the RecordSet Writer.

## Example: Remove name2 column from the response

Let's say we have the following record structure:

```
RecordSet (
  Record (
    Field "name1" = "value1",
    Field "name2" = 42
  ),
  Record (
    Field "name1" = "value2",
    Field "name2" = 84
  )
)
```

If you would like to remove the "name2" column from the response, then you need to define the following writer schema:

```json
{                   "name": "test",                   "namespace": "nifi",                   "type": "record",                   "fields": [                     { "name": "name1", "type": "string" }                 ]                 }
```