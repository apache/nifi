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

FROM openjdk:8-jre-alpine
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"

ARG UID=1000
ARG GID=1000
ARG NIFI_TOOLKIT_VERSION=
ARG NIFI_TOOLKIT_BINARY=nifi-toolkit-${NIFI_TOOLKIT_VERSION}-bin.tar.gz

ENV NIFI_TOOLKIT_BASE_DIR=/opt/nifi-toolkit
ENV NIFI_TOOLKIT_HOME=${NIFI_TOOLKIT_BASE_DIR}/nifi-toolkit-${NIFI_TOOLKIT_VERSION}

ADD ./sh/docker-entrypoint.sh /opt/sh/docker-entrypoint.sh

# Fix docker-entrypoint perms as per https://issues.apache.org/jira/browse/MRESOURCES-236 and Setup NiFi user
RUN apk add --update curl bash jq openssl \
    && rm -rf /var/cache/apk/* \
    && chmod +x /opt/sh/docker-entrypoint.sh \
    && addgroup -g $GID nifi \
    && adduser -D -s /bin/ash -u $UID -G nifi nifi \
    && mkdir -p ${NIFI_TOOLKIT_BASE_DIR}

ADD ${NIFI_TOOLKIT_BINARY} ${NIFI_TOOLKIT_BASE_DIR}
RUN chown -R nifi:nifi ${NIFI_TOOLKIT_BASE_DIR}

USER nifi

# Default port for TLS Toolkit CA Server
EXPOSE 8443

WORKDIR ${NIFI_TOOLKIT_HOME}

# Startup NiFi
ENTRYPOINT ["/opt/sh/docker-entrypoint.sh"]
