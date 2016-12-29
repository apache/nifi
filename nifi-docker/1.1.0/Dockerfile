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

ENV NIFI_VERSION=1.1.0
ENV NIFI_HOME=/nifi-$NIFI_VERSION
ENV NIFI_DOWNLOAD_SHA256 3190d5b27d2668cf9a61ad458f666a686b315241025b1b4fb2f4e40f121ffb81

LABEL owner=Apache\ NiFi \
      org.apache.nifi.version=$NIFI_VERSION \
      org.apache.nifi.release-date="2016-11-29"

# Download, validate, and expand Apache NiFi binary.
RUN curl -fSL "https://archive.apache.org/dist/nifi/$NIFI_VERSION/nifi-$NIFI_VERSION-bin.tar.gz" -o /nifi-$NIFI_VERSION-bin.tar.gz \
	&& echo "$NIFI_DOWNLOAD_SHA256 */nifi-$NIFI_VERSION-bin.tar.gz" | sha256sum -c - \
	&& tar -xvzf /nifi-$NIFI_VERSION-bin.tar.gz \
	&& rm /nifi-$NIFI_VERSION-bin.tar.gz

# Setup NiFi.properties
ADD nifi.properties $NIFI_HOME/conf/

# Web HTTP Port
EXPOSE 8080

# Remote Site-To-Site Port
EXPOSE 8181

# Startup NiFi
CMD $NIFI_HOME/bin/nifi.sh run