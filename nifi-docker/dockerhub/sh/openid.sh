#!/bin/bash -e

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

scripts_dir=${NIFI_HOME}/scripts

[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

echo 'Configuring environment with openId connect settings'

: ${OIDC_DISCOVERY_URL:?"Must specify discovery url for openId connect"}
: ${OIDC_CONNECT_TIMEOUT:?"Must specify openId connect timeout"}
: ${OIDC_READ_TIMEOUT:?"Must specify openId read timeout"}
: ${OIDC_CLIENT_ID:?"Must specify client Id"}
: ${OIDC_CLIENT_SECRET:?"Must specify client secret"}
#: ${OIDC_PREF_JWSALGO:?"Must specify discovery url for openId connect"}

prop_replace 'nifi.security.user.oidc.discovery.url'           "${OIDC_DISCOVERY_URL}"
prop_replace 'nifi.security.user.oidc.connect.timeout'         "${OIDC_CONNECT_TIMEOUT}"
prop_replace 'nifi.security.user.oidc.read.timeout'            "${OIDC_READ_TIMEOUT}"
prop_replace 'nifi.security.user.oidc.client.id'               "${OIDC_CLIENT_ID}"
prop_replace 'nifi.security.user.oidc.client.secret'           "${OIDC_CLIENT_SECRET}"
prop_replace 'nifi.security.user.oidc.preferred.jwsalgorithm'  "${OIDC_PREF_JWSALGO}"

if [ -v OIDC_PROVIDER_TRUSTSTORE_PATH ] && [ ! -z "$OIDC_PROVIDER_TRUSTSTORE_PATH" ]; then
    if [ ! -f "${OIDC_PROVIDER_TRUSTSTORE_PATH}" ]; then
        echo "Truststore file specified (${OIDC_PROVIDER_TRUSTSTORE_PATH}) does not exist."
        exit 1
    fi
    : ${OIDC_PROVIDER_TRUSTSTORE_PASSWD:?"Must specify password for turststore."}
    echo  "Setting env for self-signed certificate."
    echo  "java.arg.100=-Djavax.net.ssl.trustStore=${OIDC_PROVIDER_TRUSTSTORE_PATH}" >> ${NIFI_HOME}/conf/bootstrap.conf
    echo  "java.arg.101=-Djavax.net.ssl.trustStorePassword=${OIDC_PROVIDER_TRUSTSTORE_PASSWD}" >> ${NIFI_HOME}/conf/bootstrap.conf
else
    echo  "INFO: No self-signed certificate provided."
fi
