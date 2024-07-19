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

container_name="nifi-${TAG}-integration-test"
image_name="apache/nifi:${TAG}"
port=8443

trap '{ docker logs "${container_name}" | tail -10; docker inspect -f "{{json .State}}" "${container_name}"; docker rm -f "${container_name}"; }' EXIT

echo "Deleting any existing ${container_name} containers"
docker rm -f "${container_name}"
echo

echo "Checking that all files are owned by NiFi"
test -z "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c "find /opt/nifi ! -user nifi")"
echo

echo "Checking environment variables"
test "/opt/nifi/nifi-current" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${NIFI_HOME}')"
test "/opt/nifi/nifi-current" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c "readlink \${NIFI_BASE_DIR}/nifi-${VERSION}")"
test "/opt/nifi/nifi-toolkit-current" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c "readlink \${NIFI_BASE_DIR}/nifi-toolkit-${VERSION}")"

test "/opt/nifi/nifi-current/logs" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${NIFI_LOG_DIR}')"
test "/opt/nifi/nifi-current/run" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${NIFI_PID_DIR}')"
test "/opt/nifi" = "$(docker run --rm --entrypoint /bin/bash "${image_name}" -c 'echo -n ${NIFI_BASE_DIR}')"
echo

echo "Starting NiFi container..."
docker run -d --name "${container_name}" "${image_name}"
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${container_name}")
echo

max_iterations=10
sleep_time=10

sleep ${sleep_time}
for i in $(seq 1 "${max_iterations}") :; do
    echo "Waiting for NiFi startup - iteration: ${i}"
    if docker exec "${container_name}" bash -c " echo Running < /dev/tcp/${ip}/${port}"; then
        echo "NiFi found active on port ${port}"
        break
    fi
    echo
    if [ "${i}" -eq "${max_iterations}" ]; then
      echo "NiFi did not start within expected time"
      exit 1
    fi
    sleep 10
done
echo

echo "Checking NiFi REST API Access (expect status: 400)"
# Return code is 400 instead of 200 because of an invalid SNI
test "400" = "$(docker exec "${container_name}" bash -c "curl -ksSo /dev/null -w %{http_code} -m 10 --retry 5 --retry-connrefused --retry-max-time 60 https://${ip}:${port}/nifi-api/authentication/configuration")"
echo

echo "Stopping NiFi container"
time docker stop "${container_name}"
echo
