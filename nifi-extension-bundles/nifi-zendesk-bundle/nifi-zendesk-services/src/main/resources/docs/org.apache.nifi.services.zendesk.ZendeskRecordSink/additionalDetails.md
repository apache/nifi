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

# ZendeskRecordSink

### Description

The sink uses the Zendesk API to ingest tickets into Zendesk, using the incoming records to construct request objects.

### Authentication

Zendesk API uses basic authentication. Either a password or an authentication token has to be provided. In Zendesk API
Settings, it's possible to generate authentication tokens, eliminating the need for users to expose their passwords.
This approach also offers the advantage of fast token revocation when required.

### Property values

There are multiple ways of providing property values to the request object:

**Record Path:**

The property value is going to be evaluated as a record path if the value is provided inside brackets starting with
a '%'.

Example:

The incoming record look like this.

```json
{
  "record": {
    "description": "This is a sample description.",
    "issue\_type": "Immediate",
    "issue": {
      "name": "General error",
      "type": "Immediate"
    },
    "project": {
      "name": "Maintenance"
    }
  }
}
```

We are going to provide Record Path values for the _Comment Body, Subject, Priority_ and _Type_ processor attributes:

```
Comment Body : %{/record/description}
Subject : %{/record/issue/name}
Priority : %{/record/issue/type}
Type : %{/record/project/name}
```

The constructed request object that is going to be sent to the Zendesk API will look like this:

```json
{
  "comment": {
    "body": "This is a sample description."
  },
  "subject": "General error",
  "priority": "Immediate",
  "type": "Maintenance"
}
```

**Constant:**

The property value is going to be treated as a constant if the provided value doesn't match with the Record Path format.

Example:

We are going to provide constant values for the _Comment Body, Subject, Priority_ and _Type_ processor attributes:

```
Comment Body : Sample description
Subject : Sample subject
Priority : High
Type : Sample type
```

The constructed request object that is going to be sent to the Zendesk API will look like this:

```json
{
  "comment": {
    "body": "Sample description"
  },
  "subject": "Sample subject",
  "priority": "High",
  "type": "Sample type"
}
```

### Additional properties

The processor offers a set of frequently used Zendesk ticket attributes within its property list. However, users have
the flexibility to include any desired number of additional properties using dynamic properties. These dynamic
properties utilize their keys as Json Pointer, which denote the paths within the request object. Correspondingly, the
values of these dynamic properties align with the predefined property attributes. The possible Zendesk request
attributes can be found in
the [Zendesk API documentation](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/)

Property Key values:

The dynamic property key must be a valid Json Pointer value which has the following syntax rules:

* The path starts with **/**.
* Each segment is separated by **/**.
* Each segment can be interpreted as either an array index or an object key.

Example:

We are going to add a new dynamic property to the processor:

```
/request/new_object : This is a new property
/request/new_array/0 : This is a new array element
```

The constructed request object will look like this:

```json
{
  "request": {
    "new_object": "This is a new property",
    "new_array": [
      "This is a new array element"
    ]
  }
}
```

### Caching

The sink caches Zendesk tickets with the same content in order to avoid duplicate issues. The cache size and expiration
date can be set on the sink service.