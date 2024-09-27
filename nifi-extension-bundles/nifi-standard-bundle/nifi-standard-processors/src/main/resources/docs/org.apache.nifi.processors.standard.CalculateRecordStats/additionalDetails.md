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

# CalculateRecordStats

This processor takes in a record set and counts both the overall count and counts that are defined as dynamic properties
that map a property name to a record path. Record path counts are provided at two levels:

* The overall count of all records that successfully evaluated a record path.
* A breakdown of counts of unique values that matched the record path operation.

Consider the following record structure:

```json
{
  "sport": "Soccer",
  "name": "John Smith"
}
```

A valid mapping here would be _sport => /sport_.

For a record set with JSON like that, five entries and 3 instances of soccer and two instances of football, it would set
the following attributes:

* record\_count: 5
* sport: 5
* sport.Soccer: 3
* sport.Football: 2