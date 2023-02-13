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

FROM eclipse-temurin:11-jre
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID=1000
ARG GID=1000
ARG MINIFI_C2_VERSION=2.0.0
ARG MIRROR=https://archive.apache.org/dist

ENV MINIFI_C2_BASE_DIR /opt/minifi-c2
ENV MINIFI_C2_HOME ${MINIFI_C2_BASE_DIR}/minifi-c2-current
ENV MINIFI_C2_BINARY minifi-c2-${MINIFI_C2_VERSION}-bin.zip
ENV MINIFI_C2_BINARY_PATH nifi/${MINIFI_C2_VERSION}/${MINIFI_C2_BINARY}
ENV MINIFI_C2_BINARY_URL ${MIRROR}/${MINIFI_C2_BINARY_PATH}

RUN mkdir -p ${MINIFI_C2_BASE_DIR}

# Download, validate, and expand Apache MiNiFi C2 binary.
RUN apt-get update \
    && apt-get install -y zip \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fSL $MINIFI_C2_BINARY_URL -o $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY} \
    && echo "$(curl $MINIFI_C2_BINARY_URL.sha256) *$MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY}" | sha256sum -c - \
    && unzip $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY} -d $MINIFI_C2_BASE_DIR \
    && rm $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY} \
    && groupadd -g ${GID} c2 \
    && useradd --shell /bin/bash -u ${UID} -g ${GID} -m c2 \
    && ln -s ${MINIFI_C2_BASE_DIR}/minifi-c2-${MINIFI_C2_VERSION} $MINIFI_C2_HOME \
    && chown -R -L c2:c2 ${MINIFI_C2_BASE_DIR}

USER c2

#Default http port
EXPOSE 10090

WORKDIR ${MINIFI_C2_HOME}

# Startup MiNiFi C2
ENTRYPOINT ["./bin/c2.sh", "run"]
