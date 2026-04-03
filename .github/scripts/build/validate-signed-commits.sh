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

# Validates that all pull request commits are signed with a verified key.
# Requires: GH_TOKEN, REPOSITORY, PR_NUMBER

set -e

INVALID=0
RESULTS=$(gh api "repos/${REPOSITORY}/pulls/${PR_NUMBER}/commits" --jq '.[] | (.sha[:8] + " " + (.commit.verification.verified | tostring))')

while IFS= read -r LINE; do
  SHA=$(echo "${LINE}" | awk '{print $1}')
  VERIFIED=$(echo "${LINE}" | awk '{print $2}')
  if [ "${VERIFIED}" != "true" ]; then
    echo "::error::Commit ${SHA} is not signed. See https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits"
    INVALID=1
  fi
done <<< "${RESULTS}"

if [ "${INVALID}" -eq 1 ]; then
  exit 1
fi

echo "All commits are signed and verified"
