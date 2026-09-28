#!/bin/bash
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

if [ "${GITHUB_EVENT_NAME}" == "pull_request" ]; then
  START_SHA="${BASE_SHA}"
elif [[ "${BEFORE_SHA}" =~ ^0+$ ]]; then
  echo "frontend=true" >> "${GITHUB_OUTPUT}"
  exit 0
else
  START_SHA="${BEFORE_SHA}"
fi

if ! git cat-file -e "${START_SHA}^{commit}" 2>/dev/null; then
  if ! git fetch --no-tags --depth=1 origin "${START_SHA}"; then
    echo "frontend=true" >> "${GITHUB_OUTPUT}"
    exit 0
  fi
fi

if git diff --quiet "${START_SHA}" "${GITHUB_SHA}" -- nifi-frontend/; then
  echo "frontend=false" >> "${GITHUB_OUTPUT}"
else
  echo "frontend=true" >> "${GITHUB_OUTPUT}"
fi
