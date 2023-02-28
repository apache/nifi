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

scripts_dir='/opt/nifi/scripts'

# shellcheck source=./common.sh
[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

# Perform idempotent changes of configuration to support secure environments
echo 'Checking environment TLS settings present'

: "${NIFI_SECURITY_KEYSTORE:?"Must specify an absolute path to the keystore being used."}"
if [ ! -f "${NIFI_SECURITY_KEYSTORE}" ]; then
    echo "Keystore file specified (${NIFI_SECURITY_KEYSTORE}) does not exist."
    exit 1
fi
: "${NIFI_SECURITY_KEYSTORETYPE:?"Must specify the type of keystore (JKS, PKCS12, PEM) of the keystore being used."}"
: "${NIFI_SECURITY_KEYSTOREPASSWD:?"Must specify the password of the keystore being used."}"

: "${NIFI_SECURITY_TRUSTSTORE:?"Must specify an absolute path to the truststore being used."}"
if [ ! -f "${NIFI_SECURITY_TRUSTSTORE}" ]; then
    echo "Keystore file specified (${NIFI_SECURITY_TRUSTSTORE}) does not exist."
    exit 1
fi
: "${NIFI_SECURITY_TRUSTSTORETYPE:?"Must specify the type of truststore (JKS, PKCS12, PEM) of the truststore being used."}"
: "${NIFI_SECURITY_TRUSTSTOREPASSWD:?"Must specify the password of the truststore being used."}"


export NIFI_SECURITY_USER_AUTHORIZER="${NIFI_SECURITY_USER_AUTHORIZER:-managed-authorizer}"

# Establish initial user and an associated admin identity
sed -i -e 's|<property name="Initial User Identity 1"></property>|<property name="Initial User Identity 1">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  "${NIFI_HOME}/conf/authorizers.xml"
sed -i -e 's|<property name="Initial Admin Identity"></property>|<property name="Initial Admin Identity">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  "${NIFI_HOME}/conf/authorizers.xml"

if [ -n "${NODE_IDENTITY}" ]; then
    sed -i -e 's|<property name="Node Identity 1"></property>|<property name="Node Identity 1">'"${NODE_IDENTITY}"'</property>|'  "${NIFI_HOME}/conf/authorizers.xml"
fi

# shellcheck disable=SC2154
prop_replace 'proxiedEntity' "${INITIAL_ADMIN_IDENTITY}" "${nifi_toolkit_props_file}"
