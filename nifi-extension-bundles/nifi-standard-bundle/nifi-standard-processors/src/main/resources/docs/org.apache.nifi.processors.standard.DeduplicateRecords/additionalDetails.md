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

# DeduplicateRecords

# Overview

This processor provides deduplication across either a single record set file, across several files or even across an
entire data lake using a DistributedMapCacheClient controller service. In the case of the former, it uses either a
HashSet or a bloom filter to provide extremely fast in-memory calculations with a high degree of accuracy. In the latter
use case, it will use the controller service to compare a generated hash against a map cache stored in one of the
supported caching options that Apache NiFi offers.

## Configuring single file deduplication

Choose the "single file" option under the configuration property labeled "Deduplication Strategy." Then choose whether
to use a bloom filter or hash set. Be mindful to set size limits that are in line with the average size of the record
sets that you process.

## Configuring multi-file deduplication

Select the "Multiple Files" option under "Deduplication Strategy" and then configure a DistributedMapCacheClient
service. It is possible to configure a cache identifier in multiple ways:

1. Generate a hash of the entire record by specifying no dynamic properties.
2. Generate a hash using dynamic properties to specify particular fields to use.
3. Manually specify a single record path statement in the cache identifier property. Note:
    * This can be chained with #1 and #2 because it supports expression language and exposes the computed hash from #1
      or #2 as the EL variable _record.hash.value_. Example: _concat('\${some.var}', -, '\${record.hash.value}')_

## The role of dynamic properties

Dynamic properties should have a human-readable name for the property name and a record path operation for the value.
The record path operations will be used to extract values from the record to assemble a unique identifier. Here is an
example:

* firstName => /FirstName
* lastName => /LastName

Record:

```json
{
  "firstName": "John",
  "lastName": "Smith"
}
```

Will yield an identifier that has "John" and "Smith" in it before a hash is generated from the final value.

If any record path is missing, it will cause an exception to be raised and the flowfile will be sent to the failure
relationship.