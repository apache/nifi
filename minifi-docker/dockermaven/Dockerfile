# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM openjdk:8
MAINTAINER Apache MiNiFi <dev@nifi.apache.org>

ARG UID=1000
ARG GID=50
ARG MINIFI_VERSION
ARG MINIFI_BINARY

ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION

# Setup MiNiFi user
RUN groupadd -g $GID minifi || groupmod -n minifi `getent group $GID | cut -d: -f1`
RUN useradd --shell /bin/bash -u $UID -g $GID -m minifi
RUN mkdir -p $MINIFI_HOME 

ADD $MINIFI_BINARY $MINIFI_BASE_DIR
RUN chown -R minifi:minifi $MINIFI_HOME

USER minifi

# Startup MiNiFi
CMD $MINIFI_HOME/bin/minifi.sh run
