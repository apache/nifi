<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
      http://www.apache.org/licenses/LICENSE-2.0
      
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Docker Image Quickstart

  
## Building
The Docker image can be built using the following command:

    docker build -t apache/nifi-minifi-arm64:latest .

This build will result in an image tagged apache/nifi:latest

    # user @ puter in ~/Development/code/apache/nifi-minifi/minifi-docker/dockerhub_ARM64
    $ docker images
    REPOSITORY               TAG                 IMAGE ID            CREATED                 SIZE
    apache/nifi-minifi-arm64 latest                 ddc9dce6019e        About a minute ago   734MB
**Note**: The default version of NiFi specified by the Dockerfile is typically that of one that is unreleased if working from source.
To build an image for a prior released version, one can override the `NIFI_VERSION` build-arg with the following command:
    
    docker build --build-arg=MINIFI_VERSION={Desired MiNiFi Version} -t apache/nifi-minifi-arm64:latest .

## Running a container

### Supplying configuration to a container
The primary means by which a MiNiFi instance is configured is via the `config.yml` or the `bootstrap.conf`.

This can be accomplished through:
 * the use of volumes, and
  * overlaying the base image

#### Using volumes to provide configuration
The following example shows the usage of two volumes to provide both a `config.yml` and a `bootstrap.conf` to the container instance.  This makes use of configuration files on the host and maps them to be used by the MiNiFi instance.  This is helpful in scenarios where a single image is used for a variety of configurations.

    docker run -d \
        -v ~/minifi-conf/config.yml:/opt/minifi/minifi-0.5.0/conf/config.yml \
        -v ~/minifi-conf/bootstrap.conf:/opt/minifi/minifi-0.5.0/conf/bootstrap.conf \
        apache/nifi-minifi-arm64:latest
        
#### Using volumes to provide configuration
Alternatively, it is possible to create a custom image inheriting from the published image.  Creating a `Dockerfile` extending from the Apache NiFi MiNiFi base image allows users to overlay the configuration permanently into a newly built and custom image.  A simple example follows:

    FROM apache/nifi-minifi-arm64
    
    ADD config.yml /opt/minifi/minifi-0.5.0/conf/config.yml
    ADD bootstrap.conf /opt/minifi/minifi-0.5.0/conf/bootstrap.conf
    
Building this `Dockerfile` will result in a custom image with the specified configuration files incorporated into the new image.  This is best for applications where configuration is well defined and relatively static.

For more information, please consult [Dockerfile Reference: FROM](https://docs.docker.com/engine/reference/builder/#from)



