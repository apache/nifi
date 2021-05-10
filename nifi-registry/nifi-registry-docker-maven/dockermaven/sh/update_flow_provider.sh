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

providers_file=${NIFI_REGISTRY_HOME}/conf/providers.xml
property_xpath='/providers/flowPersistenceProvider'

add_property() {
  property_name=$1
  property_value=$2

  if [ -n "${property_value}" ]; then
    xmlstarlet ed --inplace --subnode "${property_xpath}" --type elem -n property -v "${property_value}" \
      -i \$prev --type attr -n name -v "${property_name}" \
      "${providers_file}"
  fi
}

xmlstarlet ed --inplace -u "${property_xpath}/property[@name='Flow Storage Directory']" -v "${NIFI_REGISTRY_FLOW_STORAGE_DIR:-./flow_storage}" "${providers_file}"

case ${NIFI_REGISTRY_FLOW_PROVIDER} in
    file)
        xmlstarlet ed --inplace -u "${property_xpath}/class" -v "org.apache.nifi.registry.provider.flow.FileSystemFlowPersistenceProvider" "${providers_file}"
        ;;
    git)
        xmlstarlet ed --inplace -u "${property_xpath}/class" -v "org.apache.nifi.registry.provider.flow.git.GitFlowPersistenceProvider" "${providers_file}"
        add_property "Remote To Push"  "${NIFI_REGISTRY_GIT_REMOTE:-}"
        add_property "Remote Access User"  "${NIFI_REGISTRY_GIT_USER:-}"
        add_property "Remote Access Password"    "${NIFI_REGISTRY_GIT_PASSWORD:-}"
        ;;
esac
