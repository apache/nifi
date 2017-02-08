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
MAINTAINER Apache NiFi <dev@nifi.apache.org>

ARG UID=1000
ARG GID=50
ARG NIFI_VERSION
ARG NIFI_BINARY

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME $NIFI_BASE_DIR/nifi-$NIFI_VERSION

# Setup NiFi user
RUN groupadd -g $GID nifi || groupmod -n nifi `getent group $GID | cut -d: -f1`
RUN useradd --shell /bin/bash -u $UID -g $GID -m nifi
RUN mkdir -p $NIFI_HOME 

ADD $NIFI_BINARY $NIFI_BASE_DIR
RUN chown -R nifi:nifi $NIFI_HOME

# Web HTTP Port
EXPOSE 8080

# Remote Site-To-Site Port
EXPOSE 8181

USER nifi

# Startup NiFi
CMD $NIFI_HOME/bin/nifi.sh run
