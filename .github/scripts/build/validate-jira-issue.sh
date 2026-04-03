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

ISSUE_KEY=$(echo "${PR_TITLE}" | grep -oP '^NIFI-[1-9][0-9]+' || true)

if [ -z "${ISSUE_KEY}" ]; then
  echo "::error::Apache NiFi Jira issue number not found in Pull Request title"
  exit 1
fi

JIRA_URL="https://issues.apache.org/jira/rest/api/2/issue/${ISSUE_KEY}"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${JIRA_URL}")

if [ "${HTTP_STATUS}" -eq 200 ]; then
  echo "Apache NiFi Jira issue ${ISSUE_KEY} found"
else
  echo "::error::Apache NiFi Jira issue ${ISSUE_KEY} not found: HTTP ${HTTP_STATUS}. Create an issue at https://issues.apache.org/jira/browse/NIFI"
  exit 1
fi
