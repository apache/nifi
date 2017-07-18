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

#!/bin/bash

DOCKER_UID=1000
if [ -n "$1" ]; then
  DOCKER_UID="$1"
fi

DOCKER_GID=1000
if [ -n "$2" ]; then
  DOCKER_GID="$2"
fi

MIRROR=https://archive.apache.org/dist
if [ -n "$3" ]; then
  MIRROR="$3"
fi

DOCKER_IMAGE="$(egrep -v '(^#|^\s*$|^\s*\t*#)' DockerImage.txt)"
NIFI_IMAGE_VERSION="$(echo $DOCKER_IMAGE | cut -d : -f 2)"
echo "Building NiFi Image: '$DOCKER_IMAGE' Version: $NIFI_IMAGE_VERSION Mirror: $MIRROR"
docker build --build-arg UID="$DOCKER_UID" --build-arg GID="$DOCKER_GID" --build-arg NIFI_VERSION="$NIFI_IMAGE_VERSION" --build-arg MIRROR="$MIRROR" -t $DOCKER_IMAGE .
