#@IgnoreInspection BashAddShebang
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

ARG UID=1000
ARG GID=1000
ARG NIFI_VERSION
ARG STATELESS_LIB_DIR
ARG WORKING_DIR

ENV NIFI_BASE_DIR /opt/nifi
ENV NIFI_HOME ${NIFI_BASE_DIR}/nifi-current


#Use Maven-Ant until Docker squash is stable
#COPY $NIFI_BINARY $NIFI_BASE_DIR
#RUN unzip ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip -d ${NIFI_BASE_DIR} \
#    && rm ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION}-bin.zip \
#    && mv ${NIFI_BASE_DIR}/nifi-${NIFI_VERSION} ${NIFI_HOME}
#
#COPY $NIFI_STATELESS_BINARY $NIFI_BASE_DIR
#RUN unzip ${NIFI_BASE_DIR}/nifi-stateless-${NIFI_VERSION}-bin.zip -d ${NIFI_BASE_DIR} \
#    && rm ${NIFI_BASE_DIR}/nifi-stateless-${NIFI_VERSION}-bin.zip \
#    && mv ${NIFI_BASE_DIR}/nifi-stateless-${NIFI_VERSION}/lib ${NIFI_HOME}/stateless-lib \
#    && rm -r ${NIFI_BASE_DIR}/nifi-stateless-${NIFI_VERSION}
#
#RUN java -cp "${NIFI_HOME}/stateless-lib/*" org.apache.nifi.stateless.NiFiStateless ExtractNars
#RUN rm -r ${NIFI_HOME}/lib

# Setup NiFi user
RUN addgroup -g ${GID} nifi && adduser -s /bin/sh -u ${UID} -G nifi -D nifi

RUN mkdir -p $NIFI_HOME && chown nifi:nifi $NIFI_HOME
RUN mkdir -p ${NIFI_HOME}/work/ && chown nifi:nifi ${NIFI_HOME}/work/ && chmod 777 ${NIFI_HOME}/work/

COPY --chown=nifi:nifi $WORKING_DIR ${NIFI_HOME}/work/
COPY --chown=nifi:nifi $STATELESS_LIB_DIR ${NIFI_HOME}/stateless-lib/


#NiFi's HDFS processors require core-site.xml or hdfs-site.xml to exist on disk before they can be started...
RUN echo '<configuration> \n\
                <property> \n\
                    <name>fs.defaultFS</name> \n\
                    <value>hdfs://localhost:8020</value> \n\
                </property> \n\
                <property> \n\
                    <name>fs.hdfs.impl</name> \n\
                    <value>org.apache.hadoop.hdfs.DistributedFileSystem</value> \n\
                </property> \n\
            </configuration>' > /tmp/core-site.xml && chown nifi /tmp/core-site.xml && chmod 777 /tmp/core-site.xml

RUN mkdir -p /hadoop/yarn/local && chown nifi /hadoop/yarn/local && chmod 777 /hadoop/yarn/local

USER nifi

EXPOSE 8080

WORKDIR ${NIFI_HOME}

ENTRYPOINT ["/usr/bin/java", "-cp", "stateless-lib/*", "org.apache.nifi.stateless.NiFiStateless"]
CMD ["RunOpenwhiskActionServer", "8080"]