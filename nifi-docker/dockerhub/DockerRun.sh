#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -o pipefail

DOCKER_IMAGE="$(grep -Ev '(^#|^\s*$|^\s*\t*#)' DockerImage.txt)"
NIFI_IMAGE_VERSION="$(echo "${DOCKER_IMAGE}" | cut -d : -f 2)"

echo "Running Docker Image: ${DOCKER_IMAGE}"
docker run -d --name "nifi-${NIFI_IMAGE_VERSION}" -p 8080:8080 -p 8443:8443 -p 10000:10000 -p 8000:8000 -p 8181:8181 "${DOCKER_IMAGE}"
