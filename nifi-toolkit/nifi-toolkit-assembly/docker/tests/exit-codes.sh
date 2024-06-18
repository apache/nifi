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

set -xuo pipefail

VERSION="${1}"

image_name="apache/nifi-toolkit:${VERSION}"

echo "Testing return values on missing input:"
docker run --rm "${image_name}"
test 0 -eq $? || exit 1
echo

echo "cli"
docker run --rm "${image_name}" cli invalid 1>/dev/null 2>&1
test 255 -eq $? || exit 1
echo
