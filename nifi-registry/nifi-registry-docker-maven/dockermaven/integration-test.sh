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

container_name=nifi-registry-${TAG}-integration-test

trap "{ docker ps -qaf Name=${container_name} | xargs docker rm -f; }" EXIT

echo "Checking that all files are owned by NiFi"
test -z $(docker run --rm --entrypoint /bin/bash apache/nifi-registry:${TAG} -c "find /opt/nifi-registry ! -user nifi")

echo "Checking environment variables"
test "/opt/nifi-registry/nifi-registry-current" = "$(docker run --rm --entrypoint /bin/bash apache/nifi-registry:${TAG} -c 'echo -n $NIFI_REGISTRY_HOME')"
test "/opt/nifi-registry/nifi-registry-current" = "$(docker run --rm --entrypoint /bin/bash apache/nifi-registry:${TAG} -c "readlink \${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${VERSION}")"

test "/opt/nifi-registry" = "$(docker run --rm --entrypoint /bin/bash apache/nifi-registry:${TAG} -c 'echo -n $NIFI_REGISTRY_BASE_DIR')"

echo "Starting NiFi Registry container..."

docker run -d --name ${container_name} apache/nifi-registry:${TAG}

IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${container_name})

for i in $(seq 1 10) :; do
    if docker exec ${container_name} bash -c "ss -ntl | grep 18080"; then
        break
    fi
    sleep 10
done

echo "Stopping NiFi Registry container"
time docker stop ${container_name}
