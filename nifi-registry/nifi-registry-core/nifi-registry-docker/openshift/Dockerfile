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

FROM registry.access.redhat.com/ubi8/openjdk-11
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID=1001
ARG GID=0
ARG NIFI_REGISTRY_VERSION=1.16.0
ARG MIRROR=https://archive.apache.org/dist

ENV NIFI_REGISTRY_BASE_DIR /opt/nifi-registry
ENV NIFI_REGISTRY_HOME=${NIFI_REGISTRY_BASE_DIR}/nifi-registry-current
ENV NIFI_REGISTRY_BINARY_PATH nifi/${NIFI_REGISTRY_VERSION}/nifi-registry-${NIFI_REGISTRY_VERSION}-bin.tar.gz
ENV NIFI_REGISTRY_BINARY_URL ${MIRROR}/${NIFI_REGISTRY_BINARY_PATH}

#Bugfix https://issues.redhat.com/browse/OPENJDK-335
ENV NSS_WRAPPER_PASSWD=
ENV NSS_WRAPPER_GROUP=

ADD sh/ ${NIFI_REGISTRY_BASE_DIR}/scripts/

USER root

# Setup NiFi-Registry user
RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm \
    && microdnf update \
    && microdnf install -y curl jq xmlstarlet hostname gzip nano \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m nifi \
    && mkdir -p ${NIFI_REGISTRY_BASE_DIR} \
    && chown -R ${UID}:${GID} ${NIFI_REGISTRY_BASE_DIR}

USER 1001

# Download, validate, and expand Apache NiFi-Registry binary.
RUN curl -fSL ${MIRROR}/${NIFI_REGISTRY_BINARY_URL} -o ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION}-bin.tar.gz \
    && echo "$(curl ${MIRROR}/${NIFI_REGISTRY_BINARY_URL}.sha256) *${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION}-bin.tar.gz" | sha256sum -c - \
    && tar -xvzf ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION}-bin.tar.gz -C ${NIFI_REGISTRY_BASE_DIR} \
    && rm ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION}-bin.tar.gz \
    && mv ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION} ${NIFI_REGISTRY_HOME} \
    && ln -s ${NIFI_REGISTRY_HOME} ${NIFI_REGISTRY_BASE_DIR}/nifi-registry-${NIFI_REGISTRY_VERSION} \
    && chown -R ${UID}:${GID} ${NIFI_REGISTRY_HOME}

# Web HTTP(s) ports
EXPOSE 18080 18443

WORKDIR ${NIFI_REGISTRY_HOME}

#OpenShift specific permission fixes
RUN ${NIFI_REGISTRY_BASE_DIR}/scripts/fix-permissions.sh ${NIFI_REGISTRY_BASE_DIR}

# Apply configuration and start NiFi Registry
CMD ${NIFI_REGISTRY_BASE_DIR}/scripts/start.sh
