#!/bin/sh
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

# The java implementation to use.
#export JAVA_HOME=/usr/java/jdk1.8.0/

export MINIFI_HOME=$(cd "${SCRIPT_DIR}" && cd .. && pwd)

#The directory for the MiNiFi pid file
export MINIFI_PID_DIR="${MINIFI_HOME}/run"

#The directory for MiNiFi log files
export MINIFI_LOG_DIR="${MINIFI_HOME}/logs"

# MiNiFi log file names and extensions
export MINIFI_APP_LOG_FILE_NAME=minifi-app
export MINIFI_APP_LOG_FILE_EXTENSION=log
export MINIFI_BOOTSTRAP_LOG_FILE_NAME=minifi-bootstrap
export MINIFI_BOOTSTRAP_LOG_FILE_EXTENSION=log