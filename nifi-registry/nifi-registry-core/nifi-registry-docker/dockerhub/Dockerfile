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

ARG IMAGE_NAME
ARG IMAGE_TAG
FROM ${IMAGE_NAME}:${IMAGE_TAG}
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID=1000
ARG GID=1000
ARG NIFI_REGISTRY_VERSION=2.0.0
ARG MIRROR=https://archive.apache.org/dist

ENV NIFI_REGISTRY_BASE_DIR /opt/nifi-registry
ENV NIFI_REGISTRY_HOME ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-current
ENV NIFI_REGISTRY_BINARY nifi-registry-${NIFI_REGISTRY_VERSION}-bin.zip
ENV NIFI_REGISTRY_BINARY_PATH nifi/${NIFI_REGISTRY_VERSION}/${NIFI_REGISTRY_BINARY}
ENV NIFI_REGISTRY_BINARY_URL ${MIRROR}/${NIFI_REGISTRY_BINARY_PATH}

ENV NIFI_TOOLKIT_HOME ${NIFI_REGISTRY_BASE_DIR}/nifi-toolkit-current
ENV NIFI_TOOLKIT_BINARY nifi-toolkit-${NIFI_REGISTRY_VERSION}-bin.zip
ENV NIFI_TOOLKIT_BINARY_PATH nifi/${NIFI_REGISTRY_VERSION}/${NIFI_TOOLKIT_BINARY}
ENV NIFI_TOOLKIT_BINARY_URL ${MIRROR}/${NIFI_TOOLKIT_BINARY_PATH}

ADD sh/ ${NIFI_REGISTRY_BASE_DIR}/scripts/
RUN chmod -R +x ${NIFI_REGISTRY_BASE_DIR}/scripts/*.sh

# Setup NiFi-Registry user
RUN groupadd -g ${GID} nifi || groupmod -n nifi `getent group ${GID} | cut -d: -f1` \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m nifi \
    && chown -R nifi:nifi ${NIFI_REGISTRY_BASE_DIR} \
    && apt-get update -y \
    && apt-get install -y curl jq xmlstarlet unzip \
    && apt-get -y autoremove \
    && apt-get clean autoclean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

USER nifi

# Download, validate, and expand Apache NiFi Toolkit binary.
RUN curl -fSL ${NIFI_TOOLKIT_BINARY_URL} -o ${NIFI_REGISTRY_BASE_DIR}/${NIFI_TOOLKIT_BINARY} \
    && echo "$(curl ${NIFI_TOOLKIT_BINARY_URL}.sha512) *${NIFI_REGISTRY_BASE_DIR}/${NIFI_TOOLKIT_BINARY}" | sha512sum -c - \
    && unzip ${NIFI_REGISTRY_BASE_DIR}/${NIFI_TOOLKIT_BINARY} -d ${NIFI_REGISTRY_BASE_DIR} \
    && rm ${NIFI_REGISTRY_BASE_DIR}/${NIFI_TOOLKIT_BINARY} \
    && mv ${NIFI_REGISTRY_BASE_DIR}/nifi-toolkit-${NIFI_REGISTRY_VERSION} ${NIFI_TOOLKIT_HOME} \
    && ln -s ${NIFI_TOOLKIT_HOME} ${NIFI_REGISTRY_BASE_DIR}/nifi-toolkit-${NIFI_REGISTRY_VERSION} \
    && chown -h nifi:nifi ${NIFI_TOOLKIT_HOME}

# Download, validate, and expand Apache NiFi-Registry binary.
RUN curl -fSL ${NIFI_REGISTRY_BINARY_URL} -o ${NIFI_REGISTRY_BASE_DIR}/${NIFI_REGISTRY_BINARY} \
    && echo "$(curl ${NIFI_REGISTRY_BINARY_URL}.sha512) *${NIFI_REGISTRY_BASE_DIR}/${NIFI_REGISTRY_BINARY}" | sha512sum -c - \
    && unzip ${NIFI_REGISTRY_BASE_DIR}/${NIFI_REGISTRY_BINARY} -d ${NIFI_REGISTRY_BASE_DIR} \
    && rm ${NIFI_REGISTRY_BASE_DIR}/${NIFI_REGISTRY_BINARY} \
    && mv ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION} ${NIFI_REGISTRY_HOME} \
    && ln -s ${NIFI_REGISTRY_HOME} ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION} \
    && chown -h nifi:nifi ${NIFI_REGISTRY_HOME} \
    && chown -R nifi:nifi ${NIFI_REGISTRY_HOME}

# Web HTTP(s) ports
EXPOSE 18080 18443

WORKDIR ${NIFI_REGISTRY_HOME}

# Apply configuration and start NiFi Registry
ENTRYPOINT ["../scripts/start.sh"]
