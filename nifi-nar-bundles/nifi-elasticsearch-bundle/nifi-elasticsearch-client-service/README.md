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

# Elasticsearch Client Service

## Integration Tests

The `nifi-elasticsearch-client-service` component build allows for optional Integration Tests to be executed to verify
additional functionality.

The Integration Tests create an in-memory instance of Elasticsearch, populate it with known data, perform operations
upon the instance and verify the results.

These can be activated by running the following build commands:

### Elasticsearch 5

Test integration with Elasticsearch 5.x:

```bash
mvn -P integration-tests,elasticsearch-oss clean verify
```

### Elasticsearch 6

Test integration with Elasticsearch 6.x:

```bash
mvn -P integration-tests,elasticsearch-oss,elasticsearch-6 clean verify
```

### Elasticsearch 7

[elasticsearch-oss](https://www.elastic.co/downloads/past-releases#elasticsearch-oss) was discontinued after `7.10.2`,
so the use of `elasticsearch-oss` is unnecessary for newer versions.

For 7.x, we have two separate profiles:

1. `elasticsearch-7` that can be used with `oss` (no X-Pack) and `default` (with X-Pack) flavours
2. `elasticsearch-7-no-oss` that can only be used with the `default` flavour (using a newer version of [elasticsearch](https://www.elastic.co/downloads/past-releases#elasticsearch))

#### With X-Pack

Allows for testing of some X-Pack only features such as "Point in Time" querying:

```bash
mvn -P integration-tests,elasticsearch-default,elasticsearch-7 clean verify
sleep 2
mvn -P integration-tests,elasticsearch-default,elasticsearch-7-no-oss clean verify
```

#### Without X-Pack

```bash
mvn -P integration-tests,elasticsearch-oss,elasticsearch-7 clean verify
```

### Elasticsearch 8

Test integration with Elasticsearch 8.x (with X-Pack):

```bash
mvn -P integration-tests,elasticsearch-default,elasticsearch-8 clean verify
```
