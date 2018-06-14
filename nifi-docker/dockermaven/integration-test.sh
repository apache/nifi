#!/bin/bash

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -exuo pipefail

TAG=$1
VERSION=$2

trap "{ docker rm -f nifi-${TAG}-integration-test; }" EXIT

echo "Checking that all files are owned by NiFi"
test -z $(docker run --rm --entrypoint /bin/bash apache/nifi:${TAG} -c "find /opt/nifi ! -user nifi")

echo "Checking environment variables"
test "/opt/nifi/nifi-${VERSION}" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${TAG} -c 'echo -n $NIFI_HOME')"
test "/opt/nifi/nifi-${VERSION}/logs" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${TAG} -c 'echo -n $NIFI_LOG_DIR')"
test "/opt/nifi/nifi-${VERSION}/run" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${TAG} -c 'echo -n $NIFI_PID_DIR')"
test "/opt/nifi" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${TAG} -c 'echo -n $NIFI_BASE_DIR')"

echo "Starting NiFi container..."
docker run -d --name nifi-${TAG}-integration-test apache/nifi:${TAG}

IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' nifi-${TAG}-integration-test)

for i in $(seq 1 10) :; do
    if docker exec nifi-${TAG}-integration-test bash -c "ss -ntl | grep 8080"; then
        break
    fi
    sleep 10
done

echo "Checking system diagnostics"
test ${VERSION} = $(docker exec nifi-${TAG}-integration-test bash -c "curl -s $IP:8080/nifi-api/system-diagnostics | jq .systemDiagnostics.aggregateSnapshot.versionInfo.niFiVersion -r")

echo "Stopping NiFi container"
time docker stop nifi-${TAG}-integration-test