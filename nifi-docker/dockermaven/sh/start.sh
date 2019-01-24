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

# Establish baseline properties
prop_replace 'nifi.web.http.port'               "${NIFI_WEB_HTTP_PORT:-8080}"
prop_replace 'nifi.web.http.host'               "${NIFI_WEB_HTTP_HOST:-$HOSTNAME}"
prop_replace 'nifi.remote.input.host'           "${NIFI_REMOTE_INPUT_HOST:-$HOSTNAME}"
prop_replace 'nifi.remote.input.socket.port'    "${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
prop_replace 'nifi.remote.input.secure'         'false'

# Set nifi-toolkit properties files and baseUrl
"${scripts_dir}/toolkit.sh"
prop_replace 'baseUrl' "http://${NIFI_WEB_HTTP_HOST:-$HOSTNAME}:${NIFI_WEB_HTTP_PORT:-8080}" ${nifi_toolkit_props_file}

prop_replace 'nifi.variable.registry.properties'    "${NIFI_VARIABLE_REGISTRY_PROPERTIES:-}"
prop_replace 'nifi.cluster.is.node'                         "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.node.address'                    "${NIFI_CLUSTER_ADDRESS:-$HOSTNAME}"
prop_replace 'nifi.cluster.node.protocol.port'              "${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
prop_replace 'nifi.cluster.node.protocol.threads'           "${NIFI_CLUSTER_NODE_PROTOCOL_THREADS:-10}"
prop_replace 'nifi.cluster.node.protocol.max.threads'       "${NIFI_CLUSTER_NODE_PROTOCOL_MAX_THREADS:-50}"
prop_replace 'nifi.zookeeper.connect.string'                "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node'                     "${NIFI_ZK_ROOT_NODE:-/nifi}"
prop_replace 'nifi.cluster.flow.election.max.wait.time'     "${NIFI_ELECTION_MAX_WAIT:-5 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates'    "${NIFI_ELECTION_MAX_CANDIDATES:-}"
prop_replace 'nifi.web.proxy.context.path'                  "${NIFI_WEB_PROXY_CONTEXT_PATH:-}"

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
        prop_replace 'nifi.security.user.login.identity.provider' 'ldap-provider'

        . "${scripts_dir}/secure.sh"
        . "${scripts_dir}/update_login_providers.sh"
        ;;
    *)
        if [ ! -z "${NIFI_WEB_PROXY_HOST}" ]; then
            echo 'NIFI_WEB_PROXY_HOST was set but NiFi is not configured to run in a secure mode.  Will not update nifi.web.proxy.host.'
        fi
        ;;
esac

# Continuously provide logs so that 'docker logs' can    produce them
tail -F "${NIFI_HOME}/logs/nifi-app.log" &
"${NIFI_HOME}/bin/nifi.sh" run &
nifi_pid="$!"

trap "echo Received trapped signal, beginning shutdown...;" KILL TERM HUP INT EXIT;

echo NiFi running with PID ${nifi_pid}.
wait ${nifi_pid}
