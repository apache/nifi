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

# read sensitive vales from files (if present)
. "${scripts_dir}/nifi_env_from_file.sh"

# Override JVM memory settings
if [ -n "${NIFI_JVM_HEAP_INIT}" ]; then
  # shellcheck disable=SC2154
  prop_replace 'java.arg.2' "-Xms${NIFI_JVM_HEAP_INIT}" "${nifi_bootstrap_file}"
fi

if [ -n "${NIFI_JVM_HEAP_MAX}" ]; then
  prop_replace 'java.arg.3' "-Xmx${NIFI_JVM_HEAP_MAX}" "${nifi_bootstrap_file}"
fi

if [ -n "${NIFI_JVM_DEBUGGER}" ]; then
  uncomment "java.arg.debug" "${nifi_bootstrap_file}"
fi

# set default values for some properties if not otherwise specified
export NIFI_REMOTE_INPUT_SOCKET_PORT="${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
if [ -z "${NIFI_WEB_HTTP_PORT}" ]; then
  export NIFI_WEB_HTTPS_PORT="${NIFI_WEB_HTTPS_PORT:-8443}"
  export NIFI_WEB_HTTPS_HOST="${NIFI_WEB_HTTPS_HOST:-$hostname}"
  export NIFI_WEB_HTTP_HOST=
  export BASE_URL="https://${NIFI_WEB_HTTPS_HOST}:${NIFI_WEB_HTTPS_PORT}"
  export NIFI_REMOTE_INPUT_HOST="${NIFI_REMOTE_INPUT_HOST:-$hostname}"
  export NIFI_REMOTE_INPUT_SECURE=true
  export NIFI_CLUSTER_PROTOCOL_IS_SECURE=true
  export NIFI_SECURITY_KEYSTORE="${NIFI_SECURITY_KEYSTORE:-${KEYSTORE_PATH:-${NIFI_HOME}/conf/keystore.p12}}"
  export NIFI_SECURITY_KEYSTORETYPE="${NIFI_SECURITY_KEYSTORETYPE:-${KEYSTORE_TYPE:-PKCS12}}"
  export NIFI_SECURITY_KEYSTOREPASSWD="${NIFI_SECURITY_KEYSTOREPASSWD:-${KEYSTORE_PASSWORD:-}}"
  export NIFI_SECURITY_KEYPASSWD="${NIFI_SECURITY_KEYPASSWD:-${KEY_PASSWORD:-${NIFI_SECURITY_KEYSTOREPASSWD:-}}}"
  export NIFI_SECURITY_TRUSTSTORE="${NIFI_SECURITY_KEYSTORE:-${TRUSTSTORE_PATH:-${NIFI_HOME}/conf/truststore.p12}}"
  export NIFI_SECURITY_TRUSTSTORETYPE=PKCS12
  export NIFI_SECURITY_TRUSTSTOREPASSWD="${NIFI_SECURITY_TRUSTSTOREPASSWD:-${TRUSTSTORE_PASSWORD:-}}"

  if [ -z "${NIFI_WEB_PROXY_HOST}" ]; then
    echo 'NIFI_WEB_PROXY_HOST was not set but NiFi is configured to run in a secure mode. The NiFi UI may be inaccessible if using port mapping or connecting through a proxy.'
  fi
else
  export NIFI_WEB_HTTPS_PORT=
  export NIFI_WEB_HTTPS_HOST=
  export NIFI_WEB_HTTP_HOST="${NIFI_WEB_HTTP_HOST:-$hostname}"
  export BASE_URL="http://${NIFI_WEB_HTTP_HOST}:${NIFI_WEB_HTTP_PORT}"
  export NIFI_REMOTE_INPUT_HOST="${NIFI_REMOTE_INPUT_HOST:-$hostname}"
  export NIFI_REMOTE_INPUT_SOCKET_PORT="${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
  export NIFI_REMOTE_INPUT_SECURE=false
  export NIFI_CLUSTER_PROTOCOL_IS_SECURE=false
  export NIFI_SECURITY_KEYSTORE=
  export NIFI_SECURITY_KEYSTORETYPE=
  export NIFI_SECURITY_KEYSTOREPASSWD=
  export NIFI_SECURITY_KEYPASSWD=
  export NIFI_SECURITY_TRUSTSTORE=
  export NIFI_SECURITY_TRUSTSTORETYPE=
  export NIFI_SECURITY_TRUSTSTOREPASSWD=
  export NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER=

  if [ -n "${NIFI_WEB_PROXY_HOST}" ]; then
    echo 'NIFI_WEB_PROXY_HOST was set but NiFi is not configured to run in a secure mode. Unsetting nifi.web.proxy.host.'
  fi
fi

export NIFI_VARIABLE_REGISTRY_PROPERTIES="${NIFI_VARIABLE_REGISTRY_PROPERTIES:-}"

# setup cluster properties
export NIFI_CLUSTER_IS_NODE="${NIFI_CLUSTER_IS_NODE:-false}"
export NIFI_CLUSTER_NODE_ADDRESS="${NIFI_CLUSTER_NODE_ADDRESS:-${NIFI_CLUSTER_ADDRESS:-$hostname}}"
export NIFI_CLUSTER_NODE_PROTOCOL_PORT="${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
export NIFI_CLUSTER_NODE_PROTOCOL_MAX_THREADS="${NIFI_CLUSTER_NODE_PROTOCOL_MAX_THREADS:-50}"
export NIFI_ZOOKEEPER_CONNECT_STRING="${NIFI_ZOOKEEPER_CONNECT_STRING:=${NIFI_ZK_CONNECT_STRING:-}}"
export NIFI_ZOOKEEPER_ROOT_NODE="${NIFI_ZOOKEEPER_ROOT_NODE:-${NIFI_ZK_ROOT_NODE:-/nifi}}"
export NIFI_CLUSTER_FLOW_ELECTION_MAX_WAIT_TIME="${NIFI_CLUSTER_FLOW_ELECTION_MAX_WAIT_TIME:-${NIFI_ELECTION_MAX_WAIT:-5 mins}}"
export NIFI_CLUSTER_FLOW_ELECTION_MAX_CANDIDATES="${NIFI_CLUSTER_FLOW_ELECTION_MAX_CANDIDATES:-${NIFI_ELECTION_MAX_CANDIDATES:-}}"
export NIFI_WEB_PROXY_CONTEXT_PATH="${NIFI_WEB_PROXY_CONTEXT_PATH:-}"

# Set analytics properties
export NIFI_ANALYTICS_PREDICT_ENABLED="${NIFI_ANALYTICS_PREDICT_ENABLED:-false}"
export NIFI_ANALYTICS_PREDICT_INTERVAL="${NIFI_ANALYTICS_PREDICT_INTERVAL:-3 mins}"
export NIFI_ANALYTICS_QUERY_INTERVAL="${NIFI_ANALYTICS_QUERY_INTERVAL:-5 mins}"
export NIFI_ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION="${NIFI_ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION:-${NIFI_ANALYTICS_MODEL_IMPLEMENTATION:-org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquares}}"
export NIFI_ANALYTICS_CONNECTION_MODEL_SCORE_NAME="${NIFI_ANALYTICS_CONNECTION_MODEL_SCORE_NAME:-${NIFI_ANALYTICS_MODEL_SCORE_NAME:-rSquared}}"
export NIFI_ANALYTICS_CONNECTION_MODEL_SCORE_THRESHOLD="${NIFI_ANALYTICS_CONNECTION_MODEL_SCORE_THRESHOLD:-${NIFI_ANALYTICS_MODEL_SCORE_THRESHOLD:-.90}}"

# Add NAR provider properties
export NIFI_NAR_LIBRARY_PROVIDER_NIFI__REGISTRY_URL="${NIFI_NAR_LIBRARY_PROVIDER_NIFI__REGISTRY_URL:-${NIFI_NAR_LIBRARY_PROVIDER_NIFI_REGISTRY_URL:-}}"
if [ -n "${NIFI_NAR_LIBRARY_PROVIDER_NIFI__REGISTRY_URL}" ]; then
  export NIFI_NAR_LIBRARY_PROVIDER_NIFI__REGISTRY_IMPLEMENTATION=org.apache.nifi.registry.extension.NiFiRegistryExternalResourceProvider
fi
export NIFI_NAR_LIBRARY_PROVIDER_LOCAL__FILES_SOURCE_DIR="${NIFI_NAR_LIBRARY_PROVIDER_LOCAL__FILES_SOURCE_DIR:-}"
if [ -n "${NIFI_NAR_LIBRARY_PROVIDER_LOCAL__FILES_SOURCE_DIR}" ]; then
  export NIFI_NAR_LIBRARY_PROVIDER_LOCAL__FILES_IMPLEMENTATION=org.apache.nifi.nar.provider.LocalDirectoryNarProvider
fi

# setup single user credentials (if provided)
if [ -n "${SINGLE_USER_CREDENTIALS_USERNAME}" ] && [ -n "${SINGLE_USER_CREDENTIALS_PASSWORD}" ]; then
  "${NIFI_HOME}/bin/nifi.sh" set-single-user-credentials "${SINGLE_USER_CREDENTIALS_USERNAME}" "${SINGLE_USER_CREDENTIALS_PASSWORD}"
fi

# Setup cluster state management
. "${scripts_dir}/update_cluster_state_management.sh"

# Check if we are secured or unsecured
case ${AUTH} in
tls)
  echo 'Enabling Two-Way TLS user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"
  ;;
ldap)
  echo 'Enabling LDAP user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"
  # Reference ldap-provider in properties
  export NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER="ldap-provider"

  . "${scripts_dir}/update_login_providers.sh"
  ;;
oidc)
  echo 'Enabling OIDC user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"
  # check OIDC properties are set
  . "${scripts_dir}/update_oidc_properties.sh"
  ;;
*)
  echo 'Assuming single-user authentication'
  # don't set passwords for single-user auth
  export NIFI_SECURITY_KEYSTOREPASSWD=
  export NIFI_SECURITY_KEYPASSWD=
  export NIFI_SECURITY_TRUSTSTOREPASSWD=
  ;;
esac


# Set nifi-toolkit properties files and baseUrl
"${scripts_dir}/toolkit.sh"
# shellcheck disable=SC2154
prop_replace 'baseUrl' "${BASE_URL}" "${nifi_toolkit_props_file}"
prop_replace 'keystore' "${NIFI_SECURITY_KEYSTORE}" "${nifi_toolkit_props_file}"
prop_replace 'keystoreType' "${NIFI_SECURITY_KEYSTORETYPE}" "${nifi_toolkit_props_file}"
[ -n "${NIFI_SECURITY_KEYSTOREPASSWD}" ] && prop_replace 'keystorePasswd' "${NIFI_SECURITY_KEYSTOREPASSWD}" "${nifi_toolkit_props_file}"
[ -n "${NIFI_SECURITY_KEYPASSWD}" ] && prop_replace 'keyPasswd' "${NIFI_SECURITY_KEYPASSWD}" "${nifi_toolkit_props_file}"
prop_replace 'truststore' "${NIFI_SECURITY_TRUSTSTORE}" "${nifi_toolkit_props_file}"
prop_replace 'truststoreType' "${NIFI_SECURITY_TRUSTSTORETYPE}" "${nifi_toolkit_props_file}"
[ -n "${NIFI_SECURITY_TRUSTSTOREPASSWD}" ] && prop_replace 'truststorePasswd' "${NIFI_SECURITY_TRUSTSTOREPASSWD}" "${nifi_toolkit_props_file}"


# Replace NiFi properties with environment variables
nifi_env_vars=$(printenv | awk -F= '/^NIFI_/ {print $1}' | grep -vE '^NIFI_JVM_' | grep -vE '_(HOME|DIR)$')

for nifi_env_var in ${nifi_env_vars}; do
  # mixed-case properties will be matched case-insensitively within the prop_add_or_replace/prop_replace functions
  prop_name=$(echo "${nifi_env_var}" | sed -e 's/__/-/' | tr _ . | tr '[:upper:]' '[:lower:]')
  prop_value=$(printenv "${nifi_env_var}")
  prop_add_or_replace "${prop_name}" "${prop_value}"
done

# Continuously provide logs so that 'docker logs' can produce them
"${NIFI_HOME}/bin/nifi.sh" run &
nifi_pid="$!"
tail -F --pid=${nifi_pid} "${NIFI_HOME}/logs/nifi-app.log" &

trap 'echo Received trapped signal, beginning shutdown...;./bin/nifi.sh stop;exit 0;' TERM HUP INT
trap ":" EXIT

echo NiFi running with PID ${nifi_pid}.
wait ${nifi_pid}
