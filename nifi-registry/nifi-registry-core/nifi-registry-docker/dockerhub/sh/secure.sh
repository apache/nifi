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

scripts_dir='/opt/nifi-registry/scripts'

# shellcheck source=./common.sh
[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

# Perform idempotent changes of configuration to support secure environments
echo 'Configuring environment with SSL settings'

export NIFI_REGISTRY_SECURITY_KEYSTORE="${NIFI_REGISTRY_SECURITY_KEYSTORE:-${KEYSTORE_PATH:?"Must specify an absolute path to the keystore being used."}}"
if [ ! -f "${NIFI_REGISTRY_SECURITY_KEYSTORE}" ]; then
    echo "Keystore file specified (${NIFI_REGISTRY_SECURITY_KEYSTORE}) does not exist."
    exit 1
fi
export NIFI_REGISTRY_SECURITY_KEYSTORETYPE="${NIFI_REGISTRY_SECURITY_KEYSTORETYPE:-${KEYSTORE_TYPE:?"Must specify the type of keystore (JKS, PKCS12, PEM) of the keystore being used."}}"
export NIFI_REGISTRY_SECURITY_KEYSTOREPASSWD="${NIFI_REGISTRY_SECURITY_KEYSTOREPASSWD:-${KEYSTORE_PASSWORD:?"Must specify the password of the keystore being used."}}"
export NIFI_REGISTRY_SECURITY_KEYSPASSWD="${NIFI_REGISTRY_SECURITY_KEYSPASSWD:-${KEY_PASSWORD:-${NIFI_REGISTRY_SECURITY_KEYSTOREPASSWD}}}"

export NIFI_REGISTRY_SECURITY_TRUSTSTORE="${NIFI_REGISTRY_SECURITY_TRUSTSTORE:-${TRUSTSTORE_PATH:?"Must specify an absolute path to the truststore being used."}}"
if [ ! -f "${NIFI_REGISTRY_SECURITY_TRUSTSTORE}" ]; then
    echo "Keystore file specified (${NIFI_REGISTRY_SECURITY_TRUSTSTORE}) does not exist."
    exit 1
fi
export NIFI_REGISTRY_SECURITY_TRUSTSTORETYPE="${NIFI_REGISTRY_SECURITY_TRUSTSTORETYPE:-${TRUSTSTORE_TYPE:?"Must specify the type of truststore (JKS, PKCS12, PEM) of the truststore being used."}}"
export NIFI_REGISTRY_SECURITY_TRUSTSTOREPASSWD="${NIFI_REGISTRY_SECURITY_TRUSTSTOREPASSWD:-${TRUSTSTORE_PASSWORD:?"Must specify the password of the truststore being used."}}"

# Disable HTTP and enable HTTPS
export NIFI_REGISTRY_WEB_HTTP_PORT=
export NIFI_REGISTRY_WEB_HTTP_HOST=
export NIFI_REGISTRY_WEB_HTTPS_PORT="${NIFI_REGISTRY_WEB_HTTPS_PORT:-18443}"
export NIFI_REGISTRY_WEB_HTTPS_HOST="${NIFI_REGISTRY_WEB_HTTPS_HOST:-$hostname}"

# Establish initial user and an associated admin identity
sed -i -e 's|<property name="Initial User Identity 1">.*</property>|<property name="Initial User Identity 1">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  "${NIFI_REGISTRY_HOME}/conf/authorizers.xml"
sed -i -e 's|<property name="Initial Admin Identity">.*</property>|<property name="Initial Admin Identity">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  "${NIFI_REGISTRY_HOME}/conf/authorizers.xml"
