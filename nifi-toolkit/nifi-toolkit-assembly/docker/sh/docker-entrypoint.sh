#!/bin/sh -e

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

toolkit_path="${NIFI_TOOLKIT_HOME}/bin"

program=$1

function print_help() {
    if [ -z ${program} ]; then
        echo "No program option specified."
    else
        echo "Could not find program \"${program}\" to execute."
    fi
    echo 'Options available include: ' $(for script in $(find "${toolkit_path}" -type f -name '*.sh'); do basename ${script} '.sh'; done)
}

# Find out which component of the Toolkit we are using
if ! [ -f "${toolkit_path}/${program}.sh" ]; then
    print_help ${program}
else
    shift
    exec ${toolkit_path}/${program}.sh "$@"
fi
