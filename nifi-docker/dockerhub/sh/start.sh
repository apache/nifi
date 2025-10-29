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

# Override JVM memory settings
if [ ! -z "${NIFI_JVM_HEAP_INIT}" ]; then
    prop_replace 'java.arg.2'       "-Xms${NIFI_JVM_HEAP_INIT}" ${nifi_bootstrap_file}
fi

if [ ! -z "${NIFI_JVM_HEAP_MAX}" ]; then
    prop_replace 'java.arg.3'       "-Xmx${NIFI_JVM_HEAP_MAX}" ${nifi_bootstrap_file}
fi

if [ ! -z "${NIFI_JVM_DEBUGGER}" ]; then
    uncomment "java.arg.debug" ${nifi_bootstrap_file}
fi

# Setup NiFi to use Python
uncomment "nifi.python.command" ${nifi_props_file}
prop_replace 'nifi.python.extensions.source.directory.default'  "${NIFI_HOME}/python_extensions"
# Setup NiFi to scan for new NARs in nar_extensions
prop_replace 'nifi.nar.library.autoload.directory'  "${NIFI_HOME}/nar_extensions"
# Establish baseline properties
prop_replace 'nifi.web.https.port'              "${NIFI_WEB_HTTPS_PORT:-8443}"
prop_replace 'nifi.web.https.host'              "${NIFI_WEB_HTTPS_HOST:-$HOSTNAME}"
prop_replace 'nifi.web.proxy.host'              "${NIFI_WEB_PROXY_HOST}"
prop_replace 'nifi.remote.input.host'           "${NIFI_REMOTE_INPUT_HOST:-$HOSTNAME}"
prop_replace 'nifi.remote.input.socket.port'    "${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
prop_replace 'nifi.remote.input.secure'         'true'

# Set nifi-toolkit properties files and baseUrl
"${scripts_dir}/toolkit.sh"
prop_replace 'baseUrl' "https://${NIFI_WEB_HTTPS_HOST:-$HOSTNAME}:${NIFI_WEB_HTTPS_PORT:-8443}" ${nifi_toolkit_props_file}

prop_replace 'keystore'           "${NIFI_HOME}/conf/keystore.p12"      ${nifi_toolkit_props_file}
prop_replace 'keystoreType'       "PKCS12"                              ${nifi_toolkit_props_file}
prop_replace 'truststore'         "${NIFI_HOME}/conf/truststore.p12"    ${nifi_toolkit_props_file}
prop_replace 'truststoreType'     "PKCS12"                              ${nifi_toolkit_props_file}

if [ -z "${NIFI_WEB_PROXY_HOST}" ]; then
    echo 'NIFI_WEB_PROXY_HOST was not set but NiFi is configured to run in a secure mode. The NiFi UI may be inaccessible if using port mapping or connecting through a proxy.'
fi

prop_replace 'nifi.cluster.is.node'                         "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.node.address'                    "${NIFI_CLUSTER_ADDRESS:-$HOSTNAME}"
prop_replace 'nifi.cluster.node.protocol.port'              "${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
prop_replace 'nifi.cluster.node.protocol.max.threads'       "${NIFI_CLUSTER_NODE_PROTOCOL_MAX_THREADS:-50}"
prop_replace 'nifi.cluster.load.balance.host'               "${NIFI_CLUSTER_LOAD_BALANCE_HOST:-}"
prop_replace 'nifi.zookeeper.connect.string'                "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node'                     "${NIFI_ZK_ROOT_NODE:-/nifi}"
prop_replace 'nifi.cluster.flow.election.max.wait.time'     "${NIFI_ELECTION_MAX_WAIT:-5 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates'    "${NIFI_ELECTION_MAX_CANDIDATES:-}"
prop_replace 'nifi.web.proxy.context.path'                  "${NIFI_WEB_PROXY_CONTEXT_PATH:-}"

# Set leader election and state management properties
prop_replace 'nifi.cluster.leader.election.implementation'      "${NIFI_CLUSTER_LEADER_ELECTION_IMPLEMENTATION:-CuratorLeaderElectionManager}"
prop_replace 'nifi.state.management.provider.cluster'           "${NIFI_STATE_MANAGEMENT_PROVIDER_CLUSTER:-zk-provider}"

# Set analytics properties
prop_replace 'nifi.analytics.predict.enabled'                   "${NIFI_ANALYTICS_PREDICT_ENABLED:-false}"
prop_replace 'nifi.analytics.predict.interval'                  "${NIFI_ANALYTICS_PREDICT_INTERVAL:-3 mins}"
prop_replace 'nifi.analytics.query.interval'                    "${NIFI_ANALYTICS_QUERY_INTERVAL:-5 mins}"
prop_replace 'nifi.analytics.connection.model.implementation'   "${NIFI_ANALYTICS_MODEL_IMPLEMENTATION:-org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquares}"
prop_replace 'nifi.analytics.connection.model.score.name'       "${NIFI_ANALYTICS_MODEL_SCORE_NAME:-rSquared}"
prop_replace 'nifi.analytics.connection.model.score.threshold'  "${NIFI_ANALYTICS_MODEL_SCORE_THRESHOLD:-.90}"

# Set kubernetes properties
prop_replace 'nifi.cluster.leader.election.kubernetes.lease.prefix'  "${NIFI_CLUSTER_LEADER_ELECTION_KUBERNETES_LEASE_PREFIX:-}"

# Add NAR provider properties
# nifi-registry NAR provider
if [ -n "${NIFI_NAR_LIBRARY_PROVIDER_NIFI_REGISTRY_URL}" ]; then
    prop_add_or_replace 'nifi.nar.library.provider.nifi-registry.implementation' 'org.apache.nifi.registry.extension.NiFiRegistryExternalResourceProvider'
    prop_add_or_replace 'nifi.nar.library.provider.nifi-registry.url' "${NIFI_NAR_LIBRARY_PROVIDER_NIFI_REGISTRY_URL}"
fi

if [ -n "${NIFI_SENSITIVE_PROPS_KEY}" ]; then
    prop_replace 'nifi.sensitive.props.key' "${NIFI_SENSITIVE_PROPS_KEY}"
fi

if [ -n "${SINGLE_USER_CREDENTIALS_USERNAME}" ] && [ -n "${SINGLE_USER_CREDENTIALS_PASSWORD}" ]; then
    ${NIFI_HOME}/bin/nifi.sh set-single-user-credentials "${SINGLE_USER_CREDENTIALS_USERNAME}" "${SINGLE_USER_CREDENTIALS_PASSWORD}"
fi

. "${scripts_dir}/update_cluster_state_management.sh"

# Check if we are secured or unsecured
case ${AUTH} in
    tls)
        echo 'Enabling Two-Way SSL user authentication'
        . "${scripts_dir}/secure.sh"
        ;;
    ldap)
        echo 'Enabling LDAP user authentication'
        # Reference ldap-provider in properties
        export NIFI_SECURITY_USER_LOGIN_IDENTITY_PROVIDER="ldap-provider"

        . "${scripts_dir}/secure.sh"
        . "${scripts_dir}/update_login_providers.sh"
        ;;
    oidc)
        echo 'Enabling OIDC user authentication'

        . "${scripts_dir}/secure.sh"
        . "${scripts_dir}/update_oidc_properties.sh"
        ;;
esac

# Continuously provide logs so that 'docker logs' can produce them
"${NIFI_HOME}/bin/nifi.sh" run &
nifi_pid="$!"
tail -F --pid=${nifi_pid} "${NIFI_HOME}/logs/nifi-app.log" &

trap 'echo Received trapped signal, beginning shutdown...;./bin/nifi.sh stop;exit 0;' TERM HUP INT;
trap ":" EXIT

echo NiFi running with PID ${nifi_pid}.
wait ${nifi_pid}
