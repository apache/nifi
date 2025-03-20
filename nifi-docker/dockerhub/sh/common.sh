#!/bin/sh -e
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

# 1 - value to search for
# 2 - value to replace
# 3 - file to perform replacement inline
prop_replace () {
  target_file=${3:-${nifi_props_file}}
  echo "File [${target_file}] replacing [${1}]"
  sed -i -e "s|^$1=.*$|$1=$2|"  ${target_file}
}

uncomment() {
  target_file=${2}
  echo "File [${target_file}] uncommenting [${1}]"
  sed -i -e "s|^\#$1|$1|" ${target_file}
}

# 1 - property key to add or replace
# 2 - property value to use
# 3 - file to perform replacement inline
prop_add_or_replace () {
  target_file=${3:-${nifi_props_file}}
  property_found=$(awk -v property="${1}" 'index($0, property) == 1')
  if [ -z "${property_found}" ]; then
    echo "File [${target_file}] adding [${1}]"
    echo "$1=$2" >> ${target_file}
  else
    prop_replace $1 $2 $3  
  fi
}

# NIFI_HOME is defined by an ENV command in the backing Dockerfile
export nifi_bootstrap_file=${NIFI_HOME}/conf/bootstrap.conf
export nifi_props_file=${NIFI_HOME}/conf/nifi.properties
export nifi_toolkit_props_file=${HOME}/.nifi-cli.nifi.properties
export hostname=$(hostname)

# Set Path to include local directory for Astral uv
export PATH=${PATH}:~/.local/bin
