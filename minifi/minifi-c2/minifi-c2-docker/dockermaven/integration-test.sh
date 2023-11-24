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

container_name="nifi-minifi-c2-${TAG}-integration-test"
image_name="apache/nifi-minifi-c2:${TAG}"
port=10090

trap '{ docker inspect -f "{{json .State}}" "${container_name}"; docker rm -f "${container_name}"; }' EXIT

echo "Deleting any existing ${container_name} containers"
docker rm -f "${container_name}"
echo

echo "Checking that all files are owned by C2"
test -z "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c "find /opt/minifi-c2 ! -user c2")"
echo

echo "Checking environment variables"
test "/opt/minifi-c2/minifi-c2-current" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${MINIFI_C2_HOME}')"
test "/opt/minifi-c2/minifi-c2-${VERSION}" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'readlink ${MINIFI_C2_BASE_DIR}/minifi-c2-current')"

test "/opt/minifi-c2" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${MINIFI_C2_BASE_DIR}')"
echo

echo "Starting MiNiFi C2 container..."
docker run -d --name "${container_name}" "${image_name}"
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${container_name}")
echo

max_iterations=10
sleep_time=10

sleep ${sleep_time}
for i in $(seq 1 "${max_iterations}") :; do
    echo "Waiting for MiNiFi C2 startup - iteration: ${i}"
    if docker exec "${container_name}" bash -c " echo Running < /dev/tcp/${ip}/${port}"; then
        echo "MiNiFi C2 found active on port ${port}"
        break
    fi
    echo
    if [ "${i}" -eq "${max_iterations}" ]; then
      echo "MiNiFi C2 did not start within expected time"
      exit 1
    fi
    sleep 10
done
echo

echo "Checking MiNiFi C2 Config Access (Invalid request)"
test "400" = "$(docker exec "${container_name}" bash -c "curl -sSo /dev/null -w %{http_code} -m 10 --retry 5 --retry-connrefused --retry-max-time 60 http://${ip}:${port}/c2/config")"
echo

echo "Stopping MiNiFi C2 container"
time docker stop "${container_name}"
echo
