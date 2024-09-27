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

The Syslog5424Reader Controller Service provides a means for parsing
valid [RFC 5424 Syslog](https://tools.ietf.org/html/rfc5424) messages. This service produces records with a set schema
to match the specification.

The Required Property of this service is named `Character Set` and specifies the Character Set of the incoming text.

## Schemas

When a record is parsed from incoming data, it is parsed into the RFC 5424 schema.

#### The RFC 5424 schema

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
        {
          "type": "long",
          "logicalType": "timestamp-millis"
        }
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
    },
    "name"
    :
    "appName",
    "type"
    :
    [
      "null",
      "string"
    ]
    },
    {
      "name": "procid",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "messageid",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "structuredData",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "map",
            "values": "string"
          }
        }
      ]
    }
  ]
}
```