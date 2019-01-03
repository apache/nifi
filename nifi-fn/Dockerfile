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

EXPOSE 8080
ENTRYPOINT ["/usr/bin/java", "-cp", "/usr/share/nifi-1.8.0/lib/*:/usr/share/nififn/lib/*:/usr/share/nififn/nififn.jar", "org.apache.nifi.fn.runtimes.Program"]
CMD ["RunOpenwhiskActionServer", "8080"]

# Add NiFi libraries
RUN wget -qO- https://www-us.apache.org/dist/nifi/1.8.0/nifi-1.8.0-bin.tar.gz | tar xvz -C /usr/share/

# Add Maven dependencies (not shaded into the artifact; Docker-cached)
ADD target/lib           /usr/share/nififn/lib

# Add the service itself
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/nififn/nififn.jar

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
            </configuration>' > /tmp/core-site.xml
RUN chmod 666 /tmp/core-site.xml