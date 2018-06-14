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

FROM openjdk:8-jre
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"

ARG UID=1000
ARG GID=1000
ARG NIFI_VERSION
ARG NIFI_BINARY

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME $NIFI_BASE_DIR/nifi-$NIFI_VERSION
ENV NIFI_PID_DIR=${NIFI_HOME}/run
ENV NIFI_LOG_DIR=${NIFI_HOME}/logs

ADD sh/ ${NIFI_BASE_DIR}/scripts/

ADD $NIFI_BINARY $NIFI_BASE_DIR

# Setup NiFi user and create necessary directories
RUN groupadd -g ${GID} nifi || groupmod -n nifi `getent group ${GID} | cut -d: -f1` \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m nifi \
    && mkdir -p ${NIFI_HOME}/conf/templates \
    && mkdir -p $NIFI_BASE_DIR/data \
    && mkdir -p $NIFI_BASE_DIR/flowfile_repository \
    && mkdir -p $NIFI_BASE_DIR/content_repository \
    && mkdir -p $NIFI_BASE_DIR/provenance_repository \
    && mkdir -p $NIFI_LOG_DIR \
    && chown -R nifi:nifi ${NIFI_BASE_DIR} \
    && apt-get update \
    && apt-get install -y jq xmlstarlet procps

USER nifi

# Clear nifi-env.sh in favour of configuring all environment variables in the Dockerfile
RUN echo "#!/bin/sh\n" > $NIFI_HOME/bin/nifi-env.sh

# Web HTTP(s) & Socket Site-to-Site Ports
EXPOSE 8080 8443 10000

WORKDIR ${NIFI_HOME}

# Apply configuration and start NiFi
#
# We need to use the exec form to avoid running our command in a subshell and omitting signals,
# thus being unable to shut down gracefully:
# https://docs.docker.com/engine/reference/builder/#entrypoint
#
# Also we need to use relative path, because the exec form does not invoke a command shell,
# thus normal shell processing does not happen:
# https://docs.docker.com/engine/reference/builder/#exec-form-entrypoint-example
ENTRYPOINT ["../scripts/start.sh"]
