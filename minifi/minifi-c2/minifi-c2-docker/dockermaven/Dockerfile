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

ARG UID=1000
ARG GID=1000
ARG MINIFI_C2_VERSION
ARG MINIFI_C2_BINARY_NAME
ARG MINIFI_C2_BINARY

ENV MINIFI_C2_USER c2
ENV MINIFI_C2_BASE_DIR /opt/minifi-c2
ENV MINIFI_C2_HOME ${MINIFI_C2_BASE_DIR}/minifi-c2-current

RUN mkdir -p ${MINIFI_C2_BASE_DIR}
COPY $MINIFI_C2_BINARY $MINIFI_C2_BASE_DIR

RUN apt-get update \
    && apt-get install -y unzip \
    && apt-get -y autoremove \
    && apt-get clean autoclean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && unzip ${MINIFI_C2_BASE_DIR}/${MINIFI_C2_BINARY_NAME} -d ${MINIFI_C2_BASE_DIR} \
    && rm ${MINIFI_C2_BASE_DIR}/${MINIFI_C2_BINARY_NAME} \
    && groupadd -g ${GID} ${MINIFI_C2_USER} \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m ${MINIFI_C2_USER} \
    && ln -s ${MINIFI_C2_BASE_DIR}/minifi-c2-${MINIFI_C2_VERSION} ${MINIFI_C2_HOME} \
    && chown -R -L ${MINIFI_C2_USER}:${MINIFI_C2_USER} ${MINIFI_C2_BASE_DIR}

FROM ${IMAGE_NAME}:${IMAGE_TAG}
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID
ARG GID

ENV MINIFI_C2_USER c2
ENV MINIFI_C2_BASE_DIR /opt/minifi-c2
ENV MINIFI_C2_HOME ${MINIFI_C2_BASE_DIR}/minifi-c2-current

RUN groupadd -g ${GID} ${MINIFI_C2_USER} \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m ${MINIFI_C2_USER}

COPY --from=builder ${MINIFI_C2_BASE_DIR} ${MINIFI_C2_BASE_DIR}

USER ${MINIFI_C2_USER}

#Default http port
EXPOSE 10090

WORKDIR ${MINIFI_C2_HOME}

# Startup MiNiFi C2
ENTRYPOINT ["./bin/c2.sh", "run"]
