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
ARG MINIFI_VERSION=2.0.0
ARG MIRROR=https://archive.apache.org/dist

ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME ${MINIFI_BASE_DIR}/minifi-current
ENV MINIFI_BINARY minifi-${MINIFI_VERSION}-bin.zip
ENV MINIFI_BINARY_PATH nifi/${MINIFI_VERSION}/${MINIFI_BINARY}
ENV MINIFI_BINARY_URL ${MIRROR}/${MINIFI_BINARY_PATH}

RUN mkdir -p "${MINIFI_BASE_DIR}"
ADD sh/ "${MINIFI_BASE_DIR}/scripts/"

# Download, validate, and expand Apache MiNiFi binary.
RUN apt-get update \
    && apt-get install -y zip \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fSL "${MINIFI_BINARY_URL}" -o "${MINIFI_BASE_DIR}/${MINIFI_BINARY}" \
    && echo "$(curl ${MINIFI_BINARY_URL}.sha512) *${MINIFI_BASE_DIR}/${MINIFI_BINARY}" | sha512sum -c - \
    && unzip "${MINIFI_BASE_DIR}/${MINIFI_BINARY}" -d "${MINIFI_BASE_DIR}" \
    && rm "${MINIFI_BASE_DIR}/${MINIFI_BINARY}" \
    && groupadd -g ${GID} minifi \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m minifi \
    && ln -s "${MINIFI_BASE_DIR}/minifi-${MINIFI_VERSION}" "${MINIFI_HOME}" \
    && chown -R -L minifi:minifi "${MINIFI_BASE_DIR}" \
    && chmod -R +x "${MINIFI_BASE_DIR}/scripts"/*.sh

USER minifi

WORKDIR ${MINIFI_HOME}

# Startup MiNiFi
ENTRYPOINT ["../scripts/start.sh"]
