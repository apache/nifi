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

[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

# Perform idempotent changes of configuration to support secure environments
echo 'Configuring environment with SSL settings'

: ${KEYSTORE_PATH:?"Must specify an absolute path to the keystore being used."}
if [ ! -f "${KEYSTORE_PATH}" ]; then
    echo "Keystore file specified (${KEYSTORE_PATH}) does not exist."
    exit 1
fi
: ${KEYSTORE_TYPE:?"Must specify the type of keystore (JKS, PKCS12, PEM) of the keystore being used."}
: ${KEYSTORE_PASSWORD:?"Must specify the password of the keystore being used."}

: ${TRUSTSTORE_PATH:?"Must specify an absolute path to the truststore being used."}
if [ ! -f "${TRUSTSTORE_PATH}" ]; then
    echo "Keystore file specified (${TRUSTSTORE_PATH}) does not exist."
    exit 1
fi
: ${TRUSTSTORE_TYPE:?"Must specify the type of truststore (JKS, PKCS12, PEM) of the truststore being used."}
: ${TRUSTSTORE_PASSWORD:?"Must specify the password of the truststore being used."}

prop_replace 'nifi.security.keystore'           "${KEYSTORE_PATH}"
prop_replace 'nifi.security.keystoreType'       "${KEYSTORE_TYPE}"
prop_replace 'nifi.security.keystorePasswd'     "${KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.keyPasswd'          "${KEY_PASSWORD:-$KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.truststore'         "${TRUSTSTORE_PATH}"
prop_replace 'nifi.security.truststoreType'     "${TRUSTSTORE_TYPE}"
prop_replace 'nifi.security.truststorePasswd'   "${TRUSTSTORE_PASSWORD}"

prop_replace 'keystore'           "${KEYSTORE_PATH}"                    ${nifi_toolkit_props_file}
prop_replace 'keystoreType'       "${KEYSTORE_TYPE}"                    ${nifi_toolkit_props_file}
prop_replace 'keystorePasswd'     "${KEYSTORE_PASSWORD}"                ${nifi_toolkit_props_file}
prop_replace 'keyPasswd'          "${KEY_PASSWORD:-$KEYSTORE_PASSWORD}" ${nifi_toolkit_props_file}
prop_replace 'truststore'         "${TRUSTSTORE_PATH}"                  ${nifi_toolkit_props_file}
prop_replace 'truststoreType'     "${TRUSTSTORE_TYPE}"                  ${nifi_toolkit_props_file}
prop_replace 'truststorePasswd'   "${TRUSTSTORE_PASSWORD}"              ${nifi_toolkit_props_file}

# Disable HTTP and enable HTTPS
prop_replace 'nifi.web.http.port'   ''
prop_replace 'nifi.web.http.host'   ''
prop_replace 'nifi.web.https.port'  "${NIFI_WEB_HTTPS_PORT:-8443}"
prop_replace 'nifi.web.https.host'  "${NIFI_WEB_HTTPS_HOST:-$HOSTNAME}"
prop_replace 'nifi.remote.input.secure' 'true'
# Enable the property only for cluster install
prop_replace 'nifi.cluster.protocol.is.secure' "${NIFI_CLUSTER_IS_NODE:-false}"

# Setup nifi-toolkit
prop_replace 'baseUrl' "https://${NIFI_WEB_HTTPS_HOST:-$HOSTNAME}:${NIFI_WEB_HTTPS_PORT:-8443}" ${nifi_toolkit_props_file}

# Check if the user has specified a nifi.web.proxy.host setting and handle appropriately
if [ -z "${NIFI_WEB_PROXY_HOST}" ]; then
    echo 'NIFI_WEB_PROXY_HOST was not set but NiFi is configured to run in a secure mode.  The NiFi UI may be inaccessible if using port mapping.'
else
    prop_replace 'nifi.web.proxy.host' "${NIFI_WEB_PROXY_HOST}"
fi

# Configure Authorizer and Login Identity Provider
prop_replace 'nifi.security.user.authorizer' "${NIFI_SECURITY_USER_AUTHORIZER:-managed-authorizer}"
prop_replace 'nifi.security.user.login.identity.provider' "${NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER}"

# Establish initial user and an associated admin identity
sed -i -e 's|<property name="Initial User Identity 1"></property>|<property name="Initial User Identity 1">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  ${NIFI_HOME}/conf/authorizers.xml
sed -i -e 's|<property name="Initial Admin Identity"></property>|<property name="Initial Admin Identity">'"${INITIAL_ADMIN_IDENTITY}"'</property>|'  ${NIFI_HOME}/conf/authorizers.xml

if [ -n "${NODE_IDENTITY}" ]; then
    sed -i -e 's|<property name="Node Identity 1"></property>|<property name="Node Identity 1">'"${NODE_IDENTITY}"'</property>|'  ${NIFI_HOME}/conf/authorizers.xml
fi

prop_replace 'proxiedEntity' "${INITIAL_ADMIN_IDENTITY}" ${nifi_toolkit_props_file}
