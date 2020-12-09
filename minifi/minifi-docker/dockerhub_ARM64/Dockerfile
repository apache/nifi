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

FROM arm64v8/openjdk:8
MAINTAINER Apache MiNiFi <dev@nifi.apache.org>

ARG UID=1000
ARG GID=1000
ARG MINIFI_VERSION=0.5.0

ENV MINIFI_BASE_DIR /opt/minifi
ENV MINIFI_HOME $MINIFI_BASE_DIR/minifi-current
ENV MINIFI_BINARY_URL https://archive.apache.org/dist/nifi/minifi/$MINIFI_VERSION/minifi-$MINIFI_VERSION-bin.tar.gz

# Setup MiNiFi user
RUN addgroup --gid $GID minifi || groupmod -n minifi `getent group $GID | cut -d: -f1`
RUN useradd -r -g minifi minifi
#RUN usermod -a minifi minifi
RUN mkdir -p $MINIFI_BASE_DIR

#RUN apk --no-cache add curl
RUN apt-get install curl
ADD sh/ ${MINIFI_BASE_DIR}/scripts/

# Download, validate, and expand Apache MiNiFi binary.
RUN curl -fSL $MINIFI_BINARY_URL -o $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION-bin.tar.gz \
	&& echo "$(curl $MINIFI_BINARY_URL.sha256) *$MINIFI_BASE_DIR/minifi-$MINIFI_VERSION-bin.tar.gz" | sha256sum -c - \
	&& tar -xvzf $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION-bin.tar.gz -C $MINIFI_BASE_DIR \
	&& rm $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION-bin.tar.gz \
	&& ln -s $MINIFI_BASE_DIR/minifi-$MINIFI_VERSION $MINIFI_HOME

RUN echo $MINIFI_BASE_DIR/minifi-current

RUN chown -R -L minifi:minifi $MINIFI_HOME

USER minifi

# Startup MiNiFi
CMD ${MINIFI_BASE_DIR}/scripts/start.sh
