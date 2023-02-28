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

# read sensitive vales from files (if present)
. "${scripts_dir}/nifi_registry_env_from_file.sh"

# Establish baseline properties
export NIFI_REGISTRY_WEB_HTTP_PORT="${NIFI_REGISTRY_WEB_HTTP_PORT:-18080}"
export NIFI_REGISTRY_WEB_HTTP_HOST="${NIFI_REGISTRY_WEB_HTTP_HOST:-$hostname}"

. "${scripts_dir}/update_database.sh"

# Check if we are secured or unsecured
case ${AUTH} in
tls)
  echo 'Enabling Two-Way SSL user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"
  ;;
ldap)
  echo 'Enabling LDAP user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"

  # Reference ldap-provider in properties
  export NIFI_REGISTRY_SECURITY_IDENTITY_PROVIDER=ldap-identity-provider
  export NIFI_REGISTRY_SECURITY_NEEDCLIENTAUTH=false
  . "${scripts_dir}/update_login_providers.sh"
  ;;
oidc)
  echo 'Enabling OIDC user authentication'
  # check TLS settings are set
  . "${scripts_dir}/secure.sh"
  # check OIDC properties are set
  export NIFI_REGISTRY_SECURITY_NEEDCLIENTAUTH=false
  . "${scripts_dir}/update_oidc_properties.sh"
  ;;
esac

. "${scripts_dir}/update_flow_provider.sh"
. "${scripts_dir}/update_bundle_provider.sh"


# Replace NiFi properties with environment variables
nifi_registry_env_vars=$(printenv | awk -F= '/^NIFI_REGISTRY_/ {print $1}' | grep -vE '^NIFI_REGISTRY_S3_' | grep -v '_BINARY_' | grep -vE '_(HOME|DIR)$')

for nifi_registry_env_var in ${nifi_registry_env_vars}; do
  prop_name=$(echo "${nifi_registry_env_var}" | sed -e 's/__/-/' | tr _ . | tr '[:upper:]' '[:lower:]')
  prop_value=$(printenv "${nifi_registry_env_var}")
  prop_add_or_replace "${prop_name}" "${prop_value}"
done


# Continuously provide logs so that 'docker logs' can produce them
tail -F "${NIFI_REGISTRY_HOME}/logs/nifi-registry-app.log" &
"${NIFI_REGISTRY_HOME}/bin/nifi-registry.sh" run &
nifi_registry_pid="$!"

trap "echo Received trapped signal, beginning shutdown...;" TERM HUP INT EXIT

echo NiFi-Registry running with PID ${nifi_registry_pid}.
wait ${nifi_registry_pid}
