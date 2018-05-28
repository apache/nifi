#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#!/bin/bash

#
# The script start two instances of InfluxDB. The first is unsecured and is reachable on http://localhost:8086.
# The second is secured by HTTPS and is reachable on https://localhost:9086
#
set -e

docker kill influxdb || true
docker rm influxdb || true

docker kill influxdb-secured || true
docker rm influxdb-secured || true

docker pull influxdb:latest || true

echo "Starting unsecured InfluxDB..."

docker run \
          --detach \
          --name influxdb \
          --publish 8086:8086 \
          --publish 8089:8089/udp \
          --volume ${PWD}/nifi-influxdb-services/src/test/resources/influxdb.conf:/etc/influxdb/influxdb.conf \
      influxdb:latest

echo "Starting secured InfluxDB..."

docker run \
          --detach \
          --name influxdb-secured \
          --publish 9086:9086 \
          --publish 9089:9089/udp \
          --volume ${PWD}/nifi-influxdb-services/src/test/resources/influxdb-secured.conf:/etc/influxdb/influxdb.conf \
          --volume ${PWD}/nifi-influxdb-services/src/test/resources/ssl/influxdb-selfsigned.crt:/etc/influxdb/influxdb-selfsigned.crt \
          --volume ${PWD}/nifi-influxdb-services/src/test/resources/ssl/influxdb-selfsigned.key:/etc/influxdb/influxdb-selfsigned.key \
      influxdb:latest

docker ps

echo "Run maven integration tests..."

mvn clean verify failsafe:integration-test failsafe:verify

docker kill influxdb || true
docker kill influxdb-secured || true