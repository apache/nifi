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
LABEL site="https://nifi.apache.org"

ARG UID=1000
ARG GID=50
ARG MINIFI_C2_VERSION=1.17.0
ARG MIRROR=https://archive.apache.org/dist

ENV MINIFI_C2_BASE_DIR /opt/minifi-c2
ENV MINIFI_C2_HOME ${MINIFI_C2_BASE_DIR}/minifi-c2-${MINIFI_C2_VERSION}
ENV MINIFI_C2_BINARY minifi-c2-${MINIFI_C2_VERSION}-bin.zip
ENV MINIFI_C2_BINARY_PATH nifi/${MINIFI_C2_VERSION}/${MINIFI_C2_BINARY}
ENV MINIFI_C2_BINARY_URL ${MIRROR}/${MINIFI_C2_BINARY_PATH}

# Setup MiNiFi user
RUN addgroup -g $GID c2 || groupmod -n c2 `getent group $GID | cut -d: -f1`
RUN adduser -S -H -G c2 c2
RUN mkdir -p $MINIFI_C2_HOME

# Download, validate, and expand Apache MiNiFi C2 binary.
RUN apk add curl \
    && curl -fSL $MINIFI_C2_BINARY_URL -o $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY} \
	&& echo "$(curl $MINIFI_C2_BINARY_URL.sha256) *$MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY}" | sha256sum -c - \
	&& unzip $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY} -d $MINIFI_C2_BASE_DIR \
	&& rm $MINIFI_C2_BASE_DIR/${MINIFI_C2_BINARY}

RUN chown -R c2:c2 $MINIFI_C2_HOME

USER c2

#Default http port
EXPOSE 10090

# Startup MiNiFi C2
CMD $MINIFI_C2_HOME/bin/c2.sh
