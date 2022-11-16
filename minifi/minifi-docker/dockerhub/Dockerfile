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

FROM eclipse-temurin:8-jre
LABEL maintainer="Apache NiFi <dev@nifi.apache.org>"
LABEL site="https://nifi.apache.org"

ARG UID=1000
ARG GID=1000
ARG MINIFI_VERSION=1.19.0
ARG MIRROR=https://archive.apache.org/dist

ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME ${MINIFI_BASE_DIR}/minifi-current
ENV MINIFI_BINARY minifi-${MINIFI_VERSION}-bin.zip
ENV MINIFI_BINARY_PATH nifi/${MINIFI_VERSION}/${MINIFI_BINARY}
ENV MINIFI_BINARY_URL ${MIRROR}/${MINIFI_BINARY_PATH}

# Setup MiNiFi user
RUN addgroup -gid $GID minifi || groupmod -n minifi $(getent group $GID | cut -d: -f1)
RUN useradd -r -g minifi minifi
RUN mkdir -p $MINIFI_BASE_DIR

ADD sh/ ${MINIFI_BASE_DIR}/scripts/

# Download, validate, and expand Apache MiNiFi binary.
RUN apt-get update \
    && apt-get install -y zip \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fSL $MINIFI_BINARY_URL -o $MINIFI_BASE_DIR/${MINIFI_BINARY} \
	&& echo "$(curl $MINIFI_BINARY_URL.sha256) *$MINIFI_BASE_DIR/${MINIFI_BINARY}" | sha256sum -c - \
	&& unzip $MINIFI_BASE_DIR/${MINIFI_BINARY} -d $MINIFI_BASE_DIR \
	&& rm $MINIFI_BASE_DIR/${MINIFI_BINARY} \
	&& ln -s $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION $MINIFI_HOME

RUN chown -R -L minifi:minifi $MINIFI_HOME

USER minifi

# Startup MiNiFi
CMD ${MINIFI_BASE_DIR}/scripts/start.sh
