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

scripts_dir=${NIFI_HOME}/scripts

[ -f "${scripts_dir}/common.sh" ] && . "${scripts_dir}/common.sh"

: ${KRB5_FILE_PATH:?"Must specify an absolute path to the krb5.conf"}
if [ ! -f "${KRB5_FILE_PATH}" ]; then
    echo "krb5.conf file specified (${KRB5_FILE_PATH}) does not exist."
    exit 1
fi

: ${SPNEGO_PRINCIPAL:?"Must specify service principal to it."}
: ${SPNEGO_KEYTAB_PATH:?"Must specify an absolute path to the keytab path for service principal"}
if [ ! -f "${SPNEGO_KEYTAB_PATH}" ]; then
    echo "keytab path for service principal file specified (${SPNEGO_KEYTAB_PATH}) does not exist."
    exit 1
fi
: ${SPNEGO_AUTH_EXP:?"Must specify expiration time to it"}

prop_replace 'nifi.kerberos.krb5.file'                        "${KRB5_FILE_PATH}"
prop_replace 'nifi.security.user.login.identity.provider'     "kerberos-provider"
prop_replace 'nifi.kerberos.spnego.principal'                 "${SPNEGO_PRINCIPAL}"
prop_replace 'nifi.kerberos.spnego.keytab.location'           "${SPNEGO_KEYTAB_PATH}"
prop_replace 'nifi.kerberos.spnego.authentication.expiration' "${SPNEGO_AUTH_EXP}"

: ${DEFAULT_REALM:?"Must specify Default Realm to it."}
sed -i -e '/To enable the kerberos-provider remove 2 lines/d' ${NIFI_HOME}/conf/login-identity-providers.xml
sed -i -e 's|<property name="Default Realm">NIFI.APACHE.ORG</property>|<property name="Default Realm">'"${DEFAULT_REALM}"'</property>|' ${NIFI_HOME}/conf/login-identity-providers.xml
