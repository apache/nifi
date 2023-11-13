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

container_name="nifi-minifi-${TAG}-integration-test"
image_name="apache/nifi-minifi:${TAG}"

trap '{ docker logs "${container_name}" | tail -10; docker inspect -f "{{json .State}}" "${container_name}"; docker rm -f "${container_name}"; }' EXIT

echo "Deleting any existing ${container_name} containers"
docker rm -f "${container_name}"
echo

echo "Checking that all files are owned by MiNiFi"
test -z "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c "find /opt/minifi ! -user minifi")"
echo

echo "Checking environment variables"
test "/opt/minifi/minifi-current" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${MINIFI_HOME}')"
test "/opt/minifi/minifi-${VERSION}" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'readlink ${MINIFI_BASE_DIR}/minifi-current')"

test "/opt/minifi" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${MINIFI_BASE_DIR}')"
echo

echo "Starting MiNiFi container..."
docker run -d --name "${container_name}" "${image_name}"
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${container_name}")
echo

max_iterations=10
sleep_time=10

sleep ${sleep_time}
for i in $(seq 1 "${max_iterations}") :; do
    echo "Waiting for MiNiFi startup - iteration: ${i}"
    if docker exec "${container_name}" bash -c './bin/minifi.sh status | grep -F "Apache MiNiFi is currently running"'; then
        echo "MiNiFi found active"
        break
    fi
    echo
    if [ "${i}" -eq "${max_iterations}" ]; then
      echo "MiNiFi did not start within expected time"
      exit 1
    fi
    sleep 10
done
echo

echo "Stopping MiNiFi container"
time docker stop "${container_name}"
echo
