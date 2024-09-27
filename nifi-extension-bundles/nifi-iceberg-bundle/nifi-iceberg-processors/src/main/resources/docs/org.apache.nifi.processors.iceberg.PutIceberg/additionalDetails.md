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

# PutIceberg

### Description

Iceberg is a high-performance format for huge analytic tables. The PutIceberg processor is capable of pushing data into
Iceberg tables using different types of Iceberg catalog implementations.

### Commit retry properties

Iceberg supports multiple concurrent writes using optimistic concurrency. The processor's commit retry implementation is
using **exponential backoff** with **jitter** and **scale factor 2**, and provides the following properties to configure
the behaviour according to its usage.

* Number Of Commit Retries (default: 10) - Number of retries that the processor is going to try to commit the new data
  files.
* Minimum Commit Wait Time (default: 100 ms) - Minimum time that the processor is going to wait before each commit
  attempt.
* Maximum Commit Wait Time (default: 2 sec) - Maximum time that the processor is going to wait before each commit
  attempt.
* Maximum Commit Duration (default: 30 sec) - Maximum duration that the processor is going to wait before failing the
  current processor event's commit.

The NiFi side retry logic is built on top of the Iceberg commit retry logic which can be configured through table
properties. See
more: [Table behavior properties](https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties)

### Snapshot summary properties

The processor provides an option to add additional properties to the snapshot summary using dynamic properties. The
additional property must have the 'snapshot-property.' prefix in the dynamic property key but the actual entry will be
inserted without it. Each snapshot automatically gets the FlowFile's uuid in the 'nifi-flowfile-uuid' summary property.