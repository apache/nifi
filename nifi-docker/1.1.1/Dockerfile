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

FROM java:8u91-jdk
MAINTAINER Apache NiFi <dev@nifi.apache.org>

ARG uid
ARG gid

ENV NIFI_VERSION=1.1.1
ENV NIFI_HOME=/nifi-$NIFI_VERSION
ENV NIFI_DOWNLOAD_SHA256 afb58050b0904dab329b19b14250b57d3978743a1108323bdc52aba0c2a75853

LABEL owner=Apache\ NiFi \
      org.apache.nifi.version=$NIFI_VERSION \
      org.apache.nifi.release-date="2016-12-19"

# Setup NiFi user
RUN groupadd -g $gid nifi || groupmod -n nifi `getent group $gid | cut -d: -f1`
RUN useradd --shell /bin/bash -u $uid -g $gid -m nifi
RUN mkdir -p /opt/nifi \
	&& chown -R nifi:nifi /opt/nifi

ADD docker-entrypoint.sh /home/nifi/
RUN chown -R nifi:nifi /home/nifi

# Download, validate, and expand Apache NiFi binary.
RUN curl -fSL "https://archive.apache.org/dist/nifi/$NIFI_VERSION/nifi-$NIFI_VERSION-bin.tar.gz" -o /nifi-$NIFI_VERSION-bin.tar.gz \
	&& echo "$NIFI_DOWNLOAD_SHA256 */nifi-$NIFI_VERSION-bin.tar.gz" | sha256sum -c - \
	&& tar -xvzf /nifi-$NIFI_VERSION-bin.tar.gz \
	&& rm /nifi-$NIFI_VERSION-bin.tar.gz \
	&& mv $NIFI_HOME/* /opt/nifi

# Web HTTP Port
EXPOSE 8080

# Remote Site-To-Site Port
EXPOSE 8181

USER nifi

# Startup NiFi
CMD /opt/nifi/bin/nifi.sh run
