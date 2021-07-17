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
property_xpath='/providers/extensionBundlePersistenceProvider'

add_property() {
  property_name=$1
  property_value=$2

  if [ -n "${property_value}" ]; then
    xmlstarlet ed --inplace --subnode "${property_xpath}" --type elem -n property -v "${property_value}" \
      -i \$prev --type attr -n name -v "${property_name}" \
      "${providers_file}"
  fi
}

xmlstarlet ed --inplace -u "${property_xpath}/property[@name='Extension Bundle Storage Directory']" -v "${NIFI_REGISTRY_BUNDLE_STORAGE_DIR:-./extension_bundles}" "${providers_file}"

case ${NIFI_REGISTRY_BUNDLE_PROVIDER} in
    file)
        xmlstarlet ed --inplace -u "${property_xpath}/class" -v "org.apache.nifi.registry.provider.extension.FileSystemBundlePersistenceProvider" "${providers_file}"
        ;;
    s3)
        xmlstarlet ed --inplace -u "${property_xpath}/class" -v "org.apache.nifi.registry.aws.S3BundlePersistenceProvider" "${providers_file}"
        add_property "Region"                "${NIFI_REGISTRY_S3_REGION:-}"
        add_property "Bucket Name"           "${NIFI_REGISTRY_S3_BUCKET_NAME:-}"
        add_property "Key Prefix"            "${NIFI_REGISTRY_S3_KEY_PREFIX:-}"
        add_property "Credentials Provider"  "${NIFI_REGISTRY_S3_CREDENTIALS_PROVIDER:-DEFAULT_CHAIN}"
        add_property "Access Key"            "${NIFI_REGISTRY_S3_ACCESS_KEY:-}"
        add_property "Secret Access Key"     "${NIFI_REGISTRY_S3_SECRET_ACCESS_KEY:-}"
        add_property "Endpoint URL"          "${NIFI_REGISTRY_S3_ENDPOINT_URL:-}"
        ;;
esac
