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

# Apache NiFi Docker Compose

# Usage

## Overview
The colocated docker-compose.yml is a [Docker Compose](https://docs.docker.com/compose/overview/) definition providing an easy way to start up an Apache NiFi Cluster.

## NiFi version configuration
There are two primary ways in which the cluster can be configured:

  1. By default, the Compose file (docker-compose.yml) is configured to make use of the latest image that would be created by a NiFi Maven build with the Docker profile activated through an invocation similar to `mvn clean install -Pdocker`.
  2. Optionally, a user can update the docker-compose.yml file to use a released convenience image provided in the [Apache NiFi Docker Hub repository](https://hub.docker.com/r/apache/nifi/tags/).  This is accomplished by changing the `image` property of the `nifi` service definition to be an available Docker Hub tag such as `apache/nifi:1.7.0`. **NOTE** Changes to the image can affect the expected conventions of the docker-compose.yml configuration.  All released images 1.7.0 or later should work with the file as provided.  If there are any issues, please file a [JIRA](https://issues.apache.org/jira/browse/NIFI).

## Starting the cluster
A user can start a minimal NiFi cluster with a simple `docker-compose up -d` in the `docker-compose` directory after configuring a desired NiFi version [above](#nifi-version-configuration).  This will start a NiFi cluster of one node in Compose's detached mode, with containers running in the background.

Typically, however, when clustering a user will likely want multiple instances of NiFi.  In this, more pragmatic perspective, a user would opt to instead run `docker-compose up --scale nifi=<number of instances> -d`, specifying a number of instances, such as `docker-compose up --scale nifi=3 -d`.  The `docker-compose up --scale` command can be performed at any point in the lifecycle of a running cluster instance and Compose will scale up or down to meet the number specified.  Consider the following user commands after a user starts a NiFi cluster specifying n-instances through `docker-compose up --scale nifi=n -d`:
  * `docker-compose up --scale nifi=n-m -d`:  Compose will notice that the requested number of instances (`n-m`) is less than those currently running and remove `m` instances of the `nifi` service from the running configuration,
  * `docker-compose up --scale nifi=n -d`:  Compose will notice that the requested number of instances is the number currently running and not perform any changes
  * `docker-compose up --scale nifi=n+m -d`:  Compose will notice that the requested number of instances (`n+m`) is more than those currently running and add `m` instances of the `nifi` service from the running configuration

**NOTE**:  NiFi currently does not provide any management of removed nodes from the cluster and, by virtue of how Compose works, data on those instances will be lost when instances are scaled down or removed.

## Monitoring the cluster

### Viewing logs
Logs for all running services (ZooKeeper and NiFi Nodes) can be viewed by performing `docker-compose logs` to receive a view of the collected logs thus far.  Alternatively, a user can follow the logs by adding the `-f` option such as `docker-compose logs -f`.

### Accessing the UI
To facilitate accommodating multiple instances of NiFi on one host, a fixed and exposed port is not specified in the docker-compose.yml configuration.  A user can find the host allocated port by performing `docker-compose port nifi 8080`.  This will return output similar to `0.0.0.0:32787`, indicating that the UI port, 8080, is available on all network interfaces of the host at port 32787.  If you are running the Compose environment on your local system, this would create an address of http://localhost:32787/nifi/.  If you would like to access the UI of other instances for additional evaluation, the `docker-compose port` command additionally accepts an `--index` option as follows `docker-compose port --index=<instance number> port nifi 8080`.

## Stopping the cluster
When a user has completed their usage of the cluster, all containers can be stopped by performing `docker-compose down` from the docker-compose folder. This will both stop and remove all containers associated with services in the docker-compose.yml configuration.

**NOTE**:  NiFi currently does not provide any management of removed nodes from the cluster and, by virtue of how Compose works, data on those instances will be lost when instances are scaled down or removed.




