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

ARG IMAGE_NAME=eclipse-temurin
ARG IMAGE_TAG=11-jre
FROM ${IMAGE_NAME}:${IMAGE_TAG} as builder
LABEL stage=builder

# Values are set by Maven
ARG UID
ARG GID
ARG MINIFI_VERSION
ARG MINIFI_BINARY_NAME
ARG MINIFI_BINARY
ARG MINIFI_SCRIPTS

ENV MINIFI_USER minifi
ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME ${MINIFI_BASE_DIR}/minifi-current

RUN mkdir -p ${MINIFI_BASE_DIR}
COPY ${MINIFI_BINARY} ${MINIFI_BASE_DIR}
ADD ${MINIFI_SCRIPTS} ${MINIFI_BASE_DIR}/scripts

RUN apt-get update \
    && apt-get install -y unzip \
    && apt-get -y autoremove \
    && apt-get clean autoclean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && unzip ${MINIFI_BASE_DIR}/${MINIFI_BINARY_NAME} -d ${MINIFI_BASE_DIR} \
    && rm ${MINIFI_BASE_DIR}/${MINIFI_BINARY_NAME} \
    && groupadd -g ${GID} ${MINIFI_USER} \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m ${MINIFI_USER} \
    && ln -s ${MINIFI_BASE_DIR}/minifi-${MINIFI_VERSION} ${MINIFI_HOME} \
    && chown -R ${MINIFI_USER}:${MINIFI_USER} ${MINIFI_BASE_DIR} \
    && chmod -R +x ${MINIFI_BASE_DIR}/scripts/*.sh

FROM ${IMAGE_NAME}:${IMAGE_TAG}
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID
ARG GID

ENV MINIFI_USER minifi
ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME ${MINIFI_BASE_DIR}/minifi-current

RUN groupadd -g ${GID} ${MINIFI_USER} \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m ${MINIFI_USER}

COPY --from=builder ${MINIFI_BASE_DIR} ${MINIFI_BASE_DIR}

USER ${MINIFI_USER}

WORKDIR ${MINIFI_HOME}

# Startup MiNiFi
ENTRYPOINT ["../scripts/start.sh"]
