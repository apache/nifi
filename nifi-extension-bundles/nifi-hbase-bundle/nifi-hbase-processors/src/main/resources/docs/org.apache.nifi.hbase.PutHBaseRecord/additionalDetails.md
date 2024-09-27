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

# PutHBaseRecord

## Visibility Labels:

PutHBaseRecord provides the ability to define a branch of the record as a map which contains an association between
column qualifiers and the visibility label that they should have assigned to them.

### Example Schema

```json
{
  "type": "record",
  "name": "SampleRecord",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    },
    {
      "name": "dob",
      "type": "string"
    },
    {
      "name": "attendingPhysician",
      "type": "string"
    },
    {
      "name": "accountNumber",
      "type": "string"
    },
    {
      "name": "visibility_labels",
      "type": {
        "type": "map",
        "values": "string"
      }
    }
  ]
}
```

### Example Record

```json
{
  "name": "John Smith",
  "address": "12345 Main Street",
  "dob": "1970-01-01",
  "attendingPhysician": "Dr. Jane Doe",
  "accountNumber": "1234-567-890-ABC",
  "visibility_labels": {
    "name": "OPEN",
    "address": "PII",
    "dob": "PII",
    "attendingPhysician": "PII&PHI",
    "accountNumber": "PII&BILLING"
  }
}
```

### Results in HBase

Example is for row with ID _patient-1_ and column family _patient_

| Row                                  | Value             | Visibility  |
|--------------------------------------|-------------------|-------------|
| patient-1:patient:name               | John Smith        | OPEN        |
| patient-1:patient:address            | 12345 Main Street | PII         |
| patient-1:patient:                   | 1970-01-01        | PII         |
| patient-1:patient:attendingPhysician | Dr. Jane Doe      | PII&PHI     |
| patient-1:patient:accountNumber      | 1234-567-890-ABC  | PII&BILLING |

In addition to the branch for visibility labels, the same methods used for PutHBaseCell and PutHBaseJSON can be used.
They are:

* Attributes on the flowfile.
* Dynamic properties added to the processor.

When the dynamic properties are defined on the processor, they will be the default value, but can be overridden by
attributes set on the flowfile. The naming convention for both (property name and attribute name) is:

* visibility.COLUMN\_FAMILY - every column qualifier under the column family will get this.
* visibility.COLUMN\_FAMILY.COLUMN\_VISIBILITY - the qualified column qualifier will be assigned this value.