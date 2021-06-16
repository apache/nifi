#!/usr/bin/env sh
#
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
#

# By default this file will unconditionally override whatever environment variables you have set
# and set them to defaults defined here.
# If you want to define your own versions outside of this script please set the environment variable
# NIFI_OVERRIDE_NIFIENV to "true". That will then use whatever variables you used outside of
# this script.

# The java implementation to use.
#export JAVA_HOME=/usr/java/jdk1.8.0/

setOrDefault() {
  envvar="$1"
  default="$2"

  res="$default"
  if [ -n "$envvar" ] && [ "$NIFI_OVERRIDE_NIFIENV" = "true" ]
  then
    res="$envvar"
  fi

  echo "$res"
}


NIFI_HOME="$(setOrDefault "$NIFI_HOME" "$(cd "$SCRIPT_DIR" && cd .. && pwd)")"
export NIFI_HOME

#The directory for the NiFi pid file
NIFI_PID_DIR="$(setOrDefault "$NIFI_PID_DIR" "$NIFI_HOME/run")"
export NIFI_PID_DIR

#The directory for NiFi log files
NIFI_LOG_DIR="$(setOrDefault "$NIFI_LOG_DIR" "$NIFI_HOME/logs")"
export NIFI_LOG_DIR

# Set to false to force the use of Keytab controller service in processors
# that use Kerberos. If true, these processors will allow configuration of keytab
# and principal directly within the processor. If false, these processors will be
# invalid if attempting to configure these properties. This may be advantageous in
# a multi-tenant environment where management of keytabs should be performed only by
# a user with elevated permissions (i.e., users that have been granted the 'ACCESS_KEYTAB'
# restriction).
NIFI_ALLOW_EXPLICIT_KEYTAB="$(setOrDefault "$NIFI_ALLOW_EXPLICIT_KEYTAB" true)"
export NIFI_ALLOW_EXPLICIT_KEYTAB

# Set to true to deny access to the Local File System from HDFS Processors
# This flag forces HDFS Processors to evaluate the File System path during scheduling
NIFI_HDFS_DENY_LOCAL_FILE_SYSTEM_ACCESS="$(setOrDefault "$NIFI_HDFS_DENY_LOCAL_FILE_SYSTEM_ACCESS" false)"
export NIFI_HDFS_DENY_LOCAL_FILE_SYSTEM_ACCESS
