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

[ -f /opt/nifi/scripts/common.sh ] && . /opt/nifi/scripts/common.sh

# Establish baseline properties
prop_replace 'nifi.web.http.port'  '8080'
prop_replace 'nifi.web.http.host'  "${hostname}"
prop_replace 'nifi.remote.input.host' "${hostname}"
prop_replace 'nifi.remote.input.socket.port' '10000'
prop_replace 'nifi.remote.input.secure' 'false'

# Check if we are secured or unsecured
case ${AUTH} in
    tls)
        echo 'Enabling Two-Way SSL user authentication'
        . /opt/nifi/scripts/secure.sh
        ;;
esac

# Continuously provide logs so that 'docker logs' can produce them
tail -F ${NIFI_HOME}/logs/nifi-app.log &
${NIFI_HOME}/bin/nifi.sh run &
nifi_pid="$!"

trap "echo Received trapped signal, beginning shutdown...;" KILL TERM HUP INT EXIT;

echo NiFi running with PID ${nifi_pid}.
wait ${nifi_pid}
