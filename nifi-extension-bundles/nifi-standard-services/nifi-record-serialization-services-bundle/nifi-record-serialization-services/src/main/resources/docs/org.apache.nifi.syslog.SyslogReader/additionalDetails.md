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

# Syslog5424Reader

The SyslogReader Controller Service provides a means to parse the contents of a Syslog message in accordance to RFC5424
and RFC3164 formats. This reader produces records with a set schema to match the common set of fields between the
specifications.

The Required Property of this service is named `Character Set` and specifies the Character Set of the incoming text.

## Schemas

When a record is parsed from incoming data, it is parsed into the Generic Syslog Schema.

#### The Generic Syslog Schema

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "priority",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "severity",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "facility",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "version",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "timestamp",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "hostname",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "body",
      "type": [
        "null",
        "string"
      ]
    }
  ]
}
```