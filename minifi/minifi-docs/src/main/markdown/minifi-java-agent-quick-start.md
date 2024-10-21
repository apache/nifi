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

# MiNiFi Java Agent Quick Start Guide

[Apache NiFi Team](dev@nifi.apache.org)

[NiFi Homepage](https://nifi.apache.org)

# Overview

Apache NiFi MiNiFi is an Apache NiFi project, designed to collect data at its source. MiNiFi was developed with the following objectives in mind:
* Small and lightweight footprint
* Central agent management
* Data provenance generation
* NiFi integration for follow-on dataflow management and chain of custody information

# Before You Begin
MiNiFi Java Agent is supported on the following operating systems:
* Red Hat Enterprise Linux / CentOS 7 (64-bit)
* Red Hat Enterprise Linux / CentOS 8 (64-bit)
* Ubuntu Bionic (18.04) (64-bit)
* Ubuntu Focal Fossa (20.04) (64-bit)
* Debian 9
* SUSE Linux Enterprise Server (SLES) 12 SP5 (64-bit)

You can download the MiNiFi Java Agent and the MiNiFi Toolkit from the [MiNiFi download page](https://nifi.apache.org/download/).

# Installing and Starting MiNiFi
You have several options for installing and starting MiNiFi.

## For Linux and Mac OS X Users
To install MiNiFi:
1. [Download](https://nifi.apache.org/download/) MiNiFi.
2. Extract the file to the location from which you want to the application.

You can also install MiNiFi as a service:
1. Navigate to the MiNiFi installation directory.
2. Enter:
```
bin/minifi.sh install
```
**Note:** You can also specify a custom name for your MiNiFi installation, by specifying that name during your install command. For example, to install MiNiFi as a service and named dataflow, enter:
```
bin/minifi.sh install dataflow
```
Once you have downloaded and installed MiNiFi, you need to start MiNiFi. You can start NiFi in the foreground, background, or as a service.

To launch MiNiFi in the foreground:

1. From a terminal window, navigate to the MiNiFi installation directory.
2. Enter:
```
bin/minifi.sh run
```
To launch MiNiFi in the background:

1. From a terminal window, navigate to the MiNiFi installation directory.
2. Enter:
```
bin/minifi.sh start
```
To launch MiNiFi as a service:

1. From a terminal window, enter:
```
sudo service minifi start
```
## For Windows Users
For Windows users, navigate to the folder where MiNiFi was installed. Navigate to the `/bin` subdirectory and double-click the _run-minifi.bat_ file.

This launches MiNiFi and leaves it running in the foreground. To shut down NiFi, select the window that was launched and hold the Ctrl key while pressing C.

# Working with DataFlows
When you are working with a MiNiFi dataflow, you should design it, add any additional configuration your environment or use case requires, and then deploy your dataflow. MiNiFi is not designed to accommodate substantial mid-dataflow configuration.

## Setting up Your DataFlow

### Manually from a NiFi Dataflow
You can use the MiNiFi Toolkit, located in your MiNiFi installation directory, and any NiFi instance to set up the dataflow you want MiNiFi to run:

1. Launch NiFi
2. Create a dataflow.
3. Export the dataflow in JSON format.
4. Move your new .json file to `minifi/conf`.
5. Rename your .json file _flow.json.raw_.

**Note:** You can use one template at a time, per MiNiFi instance.

**Result:** Once you have your _flow.json.raw_ file in the `minifi/conf` directory, launch that instance of MiNiFi and your dataflow begins automatically.

### Utilizing a C2 Server via the c2 protocol
If you have a C2 server running, you can expose the whole _flow.json_ for the agent to download. As the agent is heartbeating via the C2 protocol, changes in flow version will trigger automatic config updates.

1. Launch C2 server
2. Configure MiNiFi for C2 capability
```
c2.enable=true
c2.config.directory=./conf
c2.runtime.manifest.identifier=minifi
c2.runtime.type=minifi-java
c2.rest.url=http://localhost:10090/c2/config/heartbeat
c2.rest.url.ack=http://localhost:10090/c2/config/acknowledge
c2.agent.heartbeat.period=5000
#(Optional) c2.rest.callTimeout=10 sec
#(Optional) c2.agent.identifier=123-456-789
c2.agent.class=agentClassName
```
3. Start MiNiFi
4. When a new flow is available on the C2 server, MiNiFi will download it via C2 and restart itself to pick up the changes

**Note:** Flow definitions are class based. Each class has one flow defined for it. As a result, all the agents belonging to the same class will get the flow at update.<br>
**Note:** Compression can be turned on for C2 requests by setting `c2.request.compression=gzip`. Compression is turned off by default when the parameter is omitted, or when `c2.request.compression=none` is given. It can be beneficial to turn compression on to prevent network saturation.

## Loading a New Dataflow

### Manually
To load a new dataflow for a MiNiFi instance to run:

1. Create a new _flow.json.raw_ file with the new dataflow.
2. Replace the existing _flow.json.raw_ in `minifi/conf` with the new file.
3. Restart MiNiFi.

### Utilizing C2 protocol
1. Change the flow definition on the C2 Server
2. When a new flow is available on the C2 server, MiNiFi will download it via C2 and restart itself to pick up the changes

## C2 Heartbeat
Heartbeat provides status(agent, flowm device) and operational capabilities to C2 server(s)

### Agent manifest
The agent manifest is the descriptor of the available extensions. The size of the heartbeat
might increase depending on the added extensions.

With the `c2.full.heartbeat` parameter you can control whether to always include the manifest in the heartbeat or not.

The `agentInfo.agentManifestHash` node can be used to detect in the C2 server whether the manifest changed compared to the previous heartbeat.

If change is detected, a full heartbeat can be retrieved by sending a DESCRIBE MANIFEST Operation in the `requestedOperations` node of the C2 Heartbeat response.

For more details about the C2 protocol please visit [Apache NiFi - MiNiFi C2 wiki page](https://cwiki.apache.org/confluence/display/MINIFI/C2).

## Using Processors Not Packaged with MiNiFi
MiNiFi is able to use the following processors out of the box:
* AttributesToCSV
* AttributesToJSON
* CalculateRecordStats
* CompressContent
* ControlRate
* ConvertCharacterSet
* ConvertRecord
* CountText
* CryptographicHashContent
* DebugFlow
* DeduplicateRecord
* DetectDuplicate
* DistributeLoad
* DuplicateFlowFile
* EncodeContent
* EnforceOrder
* EvaluateJsonPath
* EvaluateXPath
* EvaluateXQuery
* ExecuteProcess
* ExecuteSQL
* ExecuteSQLRecord
* ExecuteStreamCommand
* ExtractGrok
* ExtractRecordSchema
* ExtractText
* FetchDistributedMapCache
* FetchFTP
* FetchFile
* FetchSFTP
* FilterAttribute
* FlattenJson
* ForkEnrichment
* ForkRecord
* GenerateFlowFile
* GenerateRecord
* GenerateTableFetch
* GetFTP
* GetFile
* GetSFTP
* HandleHttpRequest
* HandleHttpResponse
* IdentifyMimeType
* InvokeHTTP
* JoinEnrichment
* JoltTransformJSON
* ListDatabaseTables
* ListFTP
* ListFile
* ListSFTP
* ListenFTP
* ListenHTTP
* ListenSyslog
* ListenTCP
* ListenUDP
* ListenUDPRecord
* LogAttribute
* LogMessage
* LookupAttribute
* LookupRecord
* MergeContent
* MergeRecord
* ModifyBytes
* MonitorActivity
* Notify
* PackageFlowFile
* ParseSyslog
* ParseSyslog5424
* PartitionRecord
* PutDatabaseRecord
* PutDistributedMapCache
* PutEmail
* PutFTP
* PutFile
* PutRecord
* PutSFTP
* PutSQL
* PutSyslog
* PutTCP
* PutUDP
* QueryDatabaseTable
* QueryDatabaseTableRecord
* QueryRecord
* RemoveRecordField
* RenameRecordField
* ReplaceText
* ReplaceTextWithMapping
* RetryFlowFile
* RouteOnAttribute
* RouteOnContent
* RouteText
* SampleRecord
* ScanAttribute
* ScanContent
* SegmentContent
* SplitContent
* SplitJson
* SplitRecord
* SplitText
* SplitXml
* TailFile
* TransformXml
* UnpackContent
* UpdateCounter
* UpdateDatabaseTable
* UpdateRecord
* ValidateCsv
* ValidateJson
* ValidateRecord
* ValidateXml
* Wait

MiNiFi is able to use the StandardSSLContextService out of the box.

If you want to create a dataflow with a processor not shipped with MiNiFi, you can do so by following these steps:

1. Set up your dataflow as described above.
2. Copy the desired NAR file into the MiNiFi `lib` directory.
3. Restart your MiNiFi instance.

**Note:** The following processors are also a part of the default distribution but require adding a NAR for a Controller Service not packaged by default. The processors are grouped by the NAR that is required.
* nifi-dbcp-service-nar
  * PutSQL
  * GenerateTableFetch
  * ListDatabaseTables
  * QueryDatabaseTable
  * QueryDatabaseTableRecord
  * ExecuteSQL
* nifi-distributed-cache-services-nar
  * DetectDuplicate
  * FetchDistributedMapCache
  * PutDistributedMapCache
* nifi-http-context-map-nar
  * HandleHttpRequest
  * HandleHttpResponse


# Securing your Dataflow
You can secure your MiNiFi dataflow using keystore or trust store SSL protocols, however, this information is not automatically generated. You will need to generate your security configuration information yourself.

To run a MiNiFi dataflow securely:

1. Create your dataflow template as discussed above.
2. Move it to `minifi/conf` and rename _flow.json.raw_.
3. Fill in the following properties in bootstrap.conf. The flow definition will automatically pick up the necessary properties
```
nifi.minifi.security.keystore=
nifi.minifi.security.keystoreType=
nifi.minifi.security.keystorePasswd=
nifi.minifi.security.keyPasswd=
nifi.minifi.security.truststore=
nifi.minifi.security.truststoreType=
nifi.minifi.security.truststorePasswd=
nifi.minifi.security.ssl.protocol=

nifi.minifi.flow.use.parent.ssl=false

nifi.minifi.sensitive.props.key=
nifi.minifi.sensitive.props.algorithm=
```

# Managing MiNiFi
You can also perform some management tasks using MiNiFi.

## Monitoring Status
You can use the `minifi.sh flowStatus` option to monitor a range of aspects of your MiNiFi operational and dataflow status. You can use the `flowStatus` option to get information about dataflow component health and functionality, a MiNiFi instance, or system diagnostics.

FlowStatus accepts the following flags and options:
* `processor`
  * `health`
  * `bulletins`
  * `status`
* `connection`
  * `health`
  * `stats`
* `remoteProcessGroup`
  * `health`
  * `bulletins`
  * `status`
  * `authorizationIssues`
  * `inputPorts`
* `controllerServices`
  * `health`
  * `bulletins`
* `provenancereporting`
  * `health`
  * `bulletins`
* `instance`
  * `health`
  * `bulletins`
  * `status`
* `systemdiagnostics`
  * `heap`
  * `processorstats`
  * `contentrepositoryusage`
  * `flowfilerepositoryusage`
  * `garbagecollection`

For example, this query gets the health, stats, and bulletins for the TailFile processor:

```
minifi.sh flowStatus processor:TailFile:health,stats,bulletins
```

**Note:** Currently, the script only accepts one high level option at a time.

**Note:** Any connections, remote process groups or processors names that contain ":", ";" or "," will cause parsing errors when querying.

For details on the `flowStatus` option, see the "FlowStatus Query Option" section of the [Administrator's Guide](https://github.com/apache/nifi/blob/main/minifi/minifi-docs/src/main/markdown/System_Admin_Guide.md#flowstatus-query-options).

## Stopping MiNiFi

You can stop MiNiFi at any time.

Stopping MiNiFi:

1. From a terminal window, navigate to the MiNiFi installation directory.
2. Enter:
```
bin/minifi.sh stop
```

Stopping MiNiFi as a service:

1. From a terminal window, enter:
```
sudo service minifi stop
```
