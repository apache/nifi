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

export NIFI_REGISTRY_DB_URL="${NIFI_REGISTRY_DB_URL:-jdbc:h2:./database/nifi-registry-primary;AUTOCOMMIT=OFF;DB_CLOSE_ON_EXIT=FALSE;LOCK_MODE=3;LOCK_TIMEOUT=25000;WRITE_DELAY=0;AUTO_SERVER=FALSE}"
export NIFI_REGISTRY_DB_DRIVER_CLASS="${NIFI_REGISTRY_DB_DRIVER_CLASS:-${NIFI_REGISTRY_DB_CLASS:-org.h2.Driver}}"
export NIFI_REGISTRY_DB_DRIVER_DIRECTORY="${NIFI_REGISTRY_DB_DRIVER_DIRECTORY:-${NIFI_REGISTRY_DB_DIR:-}}"
export NIFI_REGISTRY_DB_USERNAME="${NIFI_REGISTRY_DB_USERNAME:-${NIFI_REGISTRY_DB_USER:-nifireg}}"
export NIFI_REGISTRY_DB_PASSWORD="${NIFI_REGISTRY_DB_PASSWORD:-${NIFI_REGISTRY_DB_PASS:-nifireg}}"
export NIFI_REGISTRY_DB_MAXCONNECTIONS="${NIFI_REGISTRY_DB_MAXCONNECTIONS:-${NIFI_REGISTRY_DB_MAX_CONNS:-5}}"
export NIFI_REGISTRY_DB_SQL_DEBUG="${NIFI_REGISTRY_DB_SQL_DEBUG:-${NIFI_REGISTRY_DB_DEBUG_SQL:-false}}"
