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

# Integration Tests

## Overview

The integration tests use [Testcontainers](https://www.testcontainers.org/) to provide a sane default for developers who have installed Docker. Testcontainers support can be disabled by setting the system property `elasticsearch.testcontainers.enabled` to something other than `true`. If Testcontainers are disabled, the endpoint will need to be configured. It can be set manually with the system property `elasticsearch.endpoint`. The default value is `http://localhost:9200`.

## Maven Profiles

* `integration-tests`
* `elasticsearch6`
* `elasticsearch7`

## Configurable System Properties

* `elasticsearch.endpoint` - Manually configure the endpoint root for a non-Docker version of Elasticsearch,
* `elasticsearch.testcontainers.enabled` - Set to anything other than `true` to disable Testcontainers and use a non-Docker version of Elasticsearch.
* `elasticsearch.elastic_user.password` - Set the Elasticsearch `elastic` user's password. When Testcontainers are enabled, this sets up the Docker container and the rest clients for accessing it within the tests. When Testcontainers are disabled, it needs to be set to whatever password is used on the external Elasticsearch node or cluster.

## Maven Run Examples

Elasticsearch 8.X is the current default version of Elasticsearch when Testcontainers are used. An example run of the integration tests with Elasticsearch 7 support would be like this:

`mvn -Pintegration-tests,elasticsearch7 --fail-at-end clean install`

An example using a non-Docker version of Elasticsearch:

`mvn -Pintegration-tests --fail-at-end -Delasticsearch.testcontainers.enabled=false -Delasticsearch.elastic_user.password=s3cret1234 clean install`

## Bash Script Example

Execute the following script from the `nifi-elasticsearch-bundle` directory:

```bash
mvn --fail-at-end -Pcontrib-check clean install

es_versions=(elasticsearch6 elasticsearch7 elasticsearch8)
it_modules=(nifi-elasticsearch-client-service nifi-elasticsearch-restapi-processors)
for v in "${es_versions[@]}"; do
    for m in "${it_modules[@]}"; do
        pushd "${m}"
        if ! mvn -P "integration-tests,${v}" --fail-at-end failsafe:integration-test failsafe:verify; then
            echo; echo; echo "Integration Tests failed for ${v} in ${m}, see Maven logs for details"
            exit 1
        fi
        popd
    done
done
```

## Modules with Integration Tests (using Testcontainers)

- [Elasticsearch Client Service](nifi-elasticsearch-client-service)
- [Elasticsearch REST API Processors](nifi-elasticsearch-restapi-processors)

## Running on Mac

Testcontainers support for "Mac OS X - Docker for Mac" is currently [best-efforts](https://www.testcontainers.org/supported_docker_environment/).

It may be necessary to do the following to run the tests successfully:

- Link the Docker Unix Socket to the standard location

    ```sh
    sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
    ```

- Set the `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` environment variable

    ```sh
    export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
    ```

## Running in IDEs

- Edit "Run Configurations" to:
  - enable Testcontainers via the system property, i.e. `-Delasticsearch.testcontainers.enabled=true`
  - set the `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` environment variable if required

## Misc

Integration Tests with Testcontainers currently only uses the `amd64` Docker Images.

`elasticsearch6` is known to experience some problems with `arm64` machines (e.g. Mac M1/M2),
but other Elasticsearch images (e.g. 7.x and 8.x) appear to work. Settings have been altered for the Elasticsearch
containers in order to try and enable them on different architectures, but there may still be some inconsistencies.

Explicit `arm64` architecture support may be added in future where the Elasticsearch images exist.
