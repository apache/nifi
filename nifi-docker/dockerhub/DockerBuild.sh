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

DOCKER_UID="${1:-1000}"
DOCKER_GID="${2:-1000}"
MIRROR="${3:-https://archive.apache.org/dist}"
BASE="${4:-${MIRROR}}"
DISTRO_PATH="${5:-}"

DOCKER_IMAGE="$(grep -Ev '(^#|^\s*$|^\s*\t*#)' DockerImage.txt)"
NIFI_IMAGE_VERSION="$(echo "${DOCKER_IMAGE}" | cut -d : -f 2)"
if [ -z "${DISTRO_PATH}" ]; then
  DISTRO_PATH="${NIFI_VERSION}"
fi

echo "Building NiFi Image: '${DOCKER_IMAGE}' Version: '${NIFI_IMAGE_VERSION}' Mirror: '${MIRROR}' Base: '${BASE} Path: '${DISTRO_PATH}' User/Group: '${DOCKER_UID}/${DOCKER_GID}'"
docker build --build-arg UID="${DOCKER_UID}" --build-arg GID="${DOCKER_GID}" --build-arg NIFI_VERSION="${NIFI_IMAGE_VERSION}" --build-arg MIRROR_BASE_URL="${MIRROR}" --build-arg BASE_URL="${BASE}" --build-arg DISTRO_PATH="${DISTRO_PATH}" -t "${DOCKER_IMAGE}" .
