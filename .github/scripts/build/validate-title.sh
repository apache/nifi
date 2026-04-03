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

# Validates that the pull request title starts with a Jira issue number.
# Requires: PR_TITLE

set -e

if echo "${PR_TITLE}" | grep -qP '^NIFI-[1-9][0-9]+'; then
  echo "Pull Request title starts with valid Apache NiFi Jira issue number"
else
  echo "::error::Pull Request title must start with an Apache NiFi Jira issue number such as NIFI-00000"
  exit 1
fi
