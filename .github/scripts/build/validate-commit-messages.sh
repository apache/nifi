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

COMMITS_JSON=$(gh api "repos/${REPOSITORY}/pulls/${PR_NUMBER}/commits")
FIRST_LINE=$(echo "${COMMITS_JSON}" | jq -r 'if length > 0 then .[0].commit.message | split("\n") | .[0] else empty end')

if [ -z "${FIRST_LINE}" ]; then
  echo "::error::No commits found for pull request"
  exit 1
fi

if ! echo "${FIRST_LINE}" | grep -qP '^NIFI-[1-9][0-9]+'; then
  echo "::error::First commit message does not start with Apache NiFi Jira issue number: ${FIRST_LINE}"
  echo "::error::The first commit message must start with an Apache NiFi Jira issue number such as NIFI-00000"
  exit 1
fi

echo "The first commit message starts with a valid Apache NiFi Jira issue number"
