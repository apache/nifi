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

# MiNiFi System Administrator's Guide

[Apache NiFi Team](dev@nifi.apache.org>)

[NiFi Homepage](http://nifi.apache.org)


# Config File

The config.yml in the _conf_ directory is the main configuration file for controlling how MiNiFi runs. This section provides an overview of the properties in this file. The file is a YAML
and follows the YAML format laid out [here](http://www.yaml.org/).

NOTE: Note that values for periods of time and data sizes must include the unit of measure,
for example "10 sec" or "10 MB", not simply "10".


## Flow Controller

The first section of config.yml is for naming and commenting on the file.

 Property | Description
 -------- | ---
name      | The name of the file.
comment   | A comment describing the usage of this config file.

## Core Properties

The Core Properties section applies to the core framework as a whole.

*Property*                                 | *Description*
---------------------------------------- | -----------
flow controller graceful shutdown period | Indicates the shutdown period. The default value is 10 sec.
flow service write delay interval        | When many changes are made to the flow.xml, this property specifies how long to wait before writing out the changes, so as to batch the changes into a single write. The default value is 500 ms.
administrative yield duration            | If a component allows an unexpected exception to escape, it is considered a bug. As a result, the framework will pause (or administratively yield) the component for this amount of time. This is done so that the component does not use up massive amounts of system resources, since it is known to have problems in the existing state. The default value is 30 sec.
bored yield duration                     | When a component has no work to do (i.e., is "bored"), this is the amount of time it will wait before checking to see if it has new data to work on. This way, it does not use up CPU resources by checking for new work too often. When setting this property, be aware that it could add extra latency for components that do not constantly have work to do, as once they go into this "bored" state, they will wait this amount of time before checking for more work. The default value is 10 millis.
max concurrent threads                   | The maximum number of threads any processor can have running at one time.

## FlowFile Repository

The FlowFile repository keeps track of the attributes and current state of each FlowFile in the system. By default,
this repository is installed in the same root installation directory as all the other repositories; however, it is advisable
to configure it on a separate drive if available.

*Property*  | *Description*
----------  | ------------
partitions  | The number of partitions. The default value is 256.
checkpoint interval | The FlowFile Repository checkpoint interval. The default value is 2 mins.
always sync | If set to _true_, any change to the repository will be synchronized to the disk, meaning that NiFi will ask the operating system not to cache the information. This is very expensive and can significantly reduce NiFi performance. However, if it is _false_, there could be the potential for data loss if either there is a sudden power loss or the operating system crashes. The default value is _false_.

#### Swap Subsection

A part of the FlowFile Repository section there is a Swap subsection.

NiFi keeps FlowFile information in memory (the JVM)
but during surges of incoming data, the FlowFile information can start to take up so much of the JVM that system performance
suffers. To counteract this effect, NiFi "swaps" the FlowFile information to disk temporarily until more JVM space becomes
available again. The "Swap" subsection of properties govern how that process occurs.

*Property*  | *Description*
----------  | ------------
threshold   | The queue threshold at which NiFi starts to swap FlowFile information to disk. The default value is 20000.
in period   | The swap in period. The default value is 5 sec.
in threads  | The number of threads to use for swapping in. The default value is 1.
out period  | The swap out period. The default value is 5 sec.
out threads | The number of threads to use for swapping out. The default value is 4.

## Content Repository

The Content Repository holds the content for all the FlowFiles in the system. By default, it is installed in the same root
installation directory as all the other repositories; however, administrators will likely want to configure it on a separate
drive if available. If nothing else, it is best if the Content Repository is not on the same drive as the FlowFile Repository.
In dataflows that handle a large amount of data, the Content Repository could fill up a disk and the
FlowFile Repository, if also on that disk, could become corrupt. To avoid this situation, configure these repositories on different drives.

*Property*                        | *Description*
--------------------------------  | -------------
content claim max appendable size | The maximum size for a content claim. The default value is 10 MB.
content claim max flow files      | The maximum number of FlowFiles to assign to one content claim. The default value is 100.
always sync                       | If set to _true_, any change to the repository will be synchronized to the disk, meaning that NiFi will ask the operating system not to cache the information. This is very expensive and can significantly reduce NiFi performance. However, if it is _false_, there could be the potential for data loss if either there is a sudden power loss or the operating system crashes. The default value is _false_.

## Provenance Repository

*Property*                        | *Description*
--------------------------------  | -------------
provenance rollover time          | The amount of time to wait before rolling over the latest data provenance information so that it is available to be accessed by components. The default value is 1 min.

## *Component Status Repository*

The Component Status Repository contains the information for the Component Status History tool in the User Interface. These
properties govern how that tool works.

The buffer.size and snapshot.frequency work together to determine the amount of historical data to retain. As an example to
configure two days worth of historical data with a data point snapshot occurring every 5 minutes you would configure
snapshot.frequency to be "5 mins" and the buffer.size to be "576". To further explain this example for every 60 minutes there
are 12 (60 / 5) snapshot windows for that time period. To keep that data for 48 hours (12 * 48) you end up with a buffer size
of 576.

*Property*        | *Description*
----------------- | -------------
buffer size       | Specifies the buffer size for the Component Status Repository. The default value is 1440.
snapshot frequency | This value indicates how often to present a snapshot of the components' status history. The default value is 1 min.

## *Security Properties*

These properties pertain to various security features in NiFi. Many of these properties are covered in more detail in the
Security Configuration section of this Administrator's Guide.

*Property*          | *Description*
------------------- | -------------
keystore            | The full path and name of the keystore. It is blank by default.
keystore type       | The keystore type. It is blank by default.
keystore password   | The keystore password. It is blank by default.
key password        | The key password. It is blank by default.
truststore          | The full path and name of the truststore. It is blank by default.
truststore type     | The truststore type. It is blank by default.
truststore password | The truststore password. It is blank by default.
ssl protocol        | The protocol to use when communicating via https. Necessary to transfer provenance securely.

#### Sensitive Properties Subsection

Some properties for processors are marked as _sensitive_ and should be encrypted. These following properties will be used to encrypt the properties while in use by MiNiFi. This will currently *not* be used to encrypt properties in the config file.

*Property* | *Description*
---------- | -------------
key        | This is the password used to encrypt any sensitive property values that are configured in processors. By default, it is blank, but the system administrator should provide a value for it. It can be a string of any length, although the recommended minimum length is 10 characters. Be aware that once this password is set and one or more sensitive processor properties have been configured, this password should not be changed.
algorithm  | The algorithm used to encrypt sensitive properties. The default value is `PBEWITHMD5AND256BITAES-CBC-OPENSSL`.
provider   | The sensitive property provider. The default value is BC.

## Processors

The current implementation of MiNiFi supports multiple processors. the "Processors" subsection is a list of these processors. Each processor must specify these properties. They are the basic configuration general to all processor implementations. Make sure that all relationships for a processor are accounted for in the auto-terminated relationship list or are used in a connection.

*Property*                          | *Description*
----------------------------------- | -------------
name                                | The name of what this processor will do. This is not used for any underlying implementation but solely for the users of this configuration and MiNiFi agent.
class                               | The fully qualified java class name of the processor to run. For example for the standard TailFile processor it would be: org.apache.nifi.processors.standard.TailFile
max concurrent tasks                | The maximum number of tasks that the processor will use.
scheduling strategy                 | The strategy for executing the processor. Valid options are `CRON_DRIVEN` or `TIMER_DRIVEN`
scheduling period                   | This property expects different input depending on the scheduling strategy selected. For the `TIMER_DRIVEN` scheduling strategy, this value is a time duration specified by a number followed by a time unit. For example, 1 second or 5 mins. The default value of 0 sec means that the Processor should run as often as possible as long as it has data to process. This is true for any time duration of 0, regardless of the time unit (i.e., 0 sec, 0 mins, 0 days). For an explanation of values that are applicable for the CRON driven scheduling strategy, see the description of the CRON driven scheduling strategy in the scheduling tab section of the [NiFi User documentation](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#scheduling-tab).
penalization period                 | Specifies how long FlowFiles will be penalized.
yield period                        | In the event the processor cannot make progress it should `yield` which will prevent the processor from being scheduled to run for some period of time. That period of time is specific using this property.
run duration nanos                  | If the processor supports batching this property can be used to control how long the Processor should be scheduled to run each time that it is triggered. Smaller values will have lower latency but larger values will have higher throughput. This period should typically only be set between 0 and 2000000000 (2 seconds).
auto-terminated relationships list  | A YAML list of the relationships to auto-terminate for the processor.

#### Processor Properties

Within the Processor Configuration section, there is the `Properties` subsection. The keys and values in this section are the property names and values for the processor. For example the TailFile processor would have a section like this:

    Properties:
        File to Tail: logs/nifi-app.log
        Rolling Filename Pattern: nifi-app*
        State File: ./conf/state/tail-file
        Initial Start Position: Beginning of File

## Connections

There can be multiple connections in this version of MiNiFi. The "Connections" subsection is a list of connections. Each connection must specify these properties.

*Property*               | *Description*
--------------------     | -------------
name                     | The name of what this connection will do. This is used for the id of the connection so it must be unique.
source name              | The name of what of the processor that is the source for this connection.
source relationship name | The name of the processors relationship to route to this connection
destination name         | The name of the component to receive this connection.
max work queue size      | This property is the max number of FlowFiles that can be in the queue before back pressure is applied. When back pressure is applied the source processor will no longer be scheduled to run.
max work queue data size | This property specifies the maximum amount of data (in size) that should be queued up before applying back pressure.  When back pressure is applied the source processor will no longer be scheduled to run.
flowfile expiration      | Indicates how long FlowFiles are allowed to exist in the connection before be expired (automatically removed from the flow).
queue prioritizer class  | This configuration option specifies the fully qualified java class path of a queue prioritizer to use. If no special prioritizer is desired then it should be left blank. An example value of this property is: org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer

## Remote Processing Groups

MiNiFi can be used to send data using the Site to Site protocol (via a Remote Processing Group) or a Processor. These properties configure the Remote Processing Groups that use Site-To-Site to send data to a core instance.

*Property*   | *Description*
------------ | -------------
name         | The name of what this Remote Processing Group points to. This is not used for any underlying implementation but solely for the users of this configuration and MiNiFi agent.
comment      | A comment about the Remote Processing Group. This is not used for any underlying implementation but solely for the users of this configuration and MiNiFi agent.
url          | The URL of the core NiFi instance.
timeout      | How long MiNiFi should wait before timing out the connection.
yield period | When communication with this Remote Processing Group fails, it will not be scheduled again for this amount of time.


#### Input Ports Subsection

When connecting via Site to Site, MiNiFi needs to know which input port to communicate to of the core NiFi instance. These properties designate and configure communication with that port.

*Property*           | *Description*
-------------------- | -------------
id                   | The id of the input port as it exists on the core NiFi instance. To get this information access the UI of the core instance, right the input port that is desired to be connect to and select "configure". The id of the port should under the "Id" section.
name                 | The name of the input port as it exists on the core NiFi instance. To get this information access the UI of the core instance, right the input port that is desired to be connect to and select "configure". The id of the port should under the "Port name" section.
comments:            | A comment about the Input Port. This is not used for any underlying implementation but solely for the users of this configuration and MiNiFi agent.
max concurrent tasks | The number of tasks that this port should be scheduled for at maximum.
use compression      | Whether or not compression should be used when communicating with the port. This is a boolean value of either "true" or "false"

## Provenance Reporting

MiNiFi is currently designed only to report provenance data using the Site to Site protocol. These properties configure the underlying reporting task that sends the provenance events.

*Property*           | *Description*
-------------------- | -------------
comment              | A comment about the Provenance reporting. This is not used for any underlying implementation but solely for the users of this configuration and MiNiFi agent.
scheduling strategy  | The strategy for executing the Reporting Task. Valid options are `CRON_DRIVEN` or `TIMER_DRIVEN`
scheduling period    | This property expects different input depending on the scheduling strategy selected. For the `TIMER_DRIVEN` scheduling strategy, this value is a time duration specified by a number followed by a time unit. For example, 1 second or 5 mins. The default value of 0 sec means that the Processor should run as often as possible as long as it has data to process. This is true for any time duration of 0, regardless of the time unit (i.e., 0 sec, 0 mins, 0 days). For an explanation of values that are applicable for the CRON driven scheduling strategy, see the description of the CRON driven scheduling strategy in the scheduling tab section of the [NiFi User documentation](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#scheduling-tab).
destination url      | The URL to post the Provenance Events to.
port name            | The name of the input port as it exists on the receiving NiFi instance. To get this information access the UI of the core instance, right the input port that is desired to be connect to and select "configure". The id of the port should under the "Port name" section.
originating url      | The URL of this MiNiFi instance. This is used to include the Content URI to send to the destination.
use compression      | Indicates whether or not to compress the events when being sent.
timeout              | How long MiNiFi should wait before timing out the connection.
batch size           | Specifies how many records to send in a single batch, at most. This should be significantly above the expected amount of records generated between scheduling. If it is not, then there is the potential for the Provenance reporting to lag behind event generation and never catch up.



# Example Config File

Below are two example config YAML files. The first tails the minifi-app.log, send the tailed log and provenance data back to a secure instance of NiFi. The second uses a series of processors to tail the app log, routes off only lines that contain "WriteAheadFlowFileRepository" and puts it as a file in the "./" directory.


``` yaml
Flow Controller:
    name: MiNiFi Flow
    comment:

Core Properties:
    flow controller graceful shutdown period: 10 sec
    flow service write delay interval: 500 ms
    administrative yield duration: 30 sec
    bored yield duration: 10 millis

FlowFile Repository:
    partitions: 256
    checkpoint interval: 2 mins
    always sync: false
    Swap:
        threshold: 20000
        in period: 5 sec
        in threads: 1
        out period: 5 sec
        out threads: 4

Provenance Repository:
    provenance rollover time: 1 min

Content Repository:
    content claim max appendable size: 10 MB
    content claim max flow files: 100
    always sync: false

Component Status Repository:
    buffer size: 1440
    snapshot frequency: 1 min

Security Properties:
    keystore: /tmp/ssl/localhost-ks.jks
    keystore type: JKS
    keystore password: localtest
    key password: localtest
    truststore: /tmp/ssl/localhost-ts.jks
    truststore type: JKS
    truststore password: localtest
    ssl protocol: TLS
    Sensitive Props:
        key:
        algorithm: PBEWITHMD5AND256BITAES-CBC-OPENSSL
        provider: BC

Processors:
    - name: TailFile
      class: org.apache.nifi.processors.standard.TailFile
      max concurrent tasks: 1
      scheduling strategy: TIMER_DRIVEN
      scheduling period: 1 sec
      penalization period: 30 sec
      yield period: 1 sec
      run duration nanos: 0
      auto-terminated relationships list:
      Properties:
          File to Tail: logs/minifi-app.log
          Rolling Filename Pattern: minifi-app*
          Initial Start Position: Beginning of File

Connections:
    - name: TailToS2S
      source name: TailFile
      source relationship name: success
      destination name: 8644cbcc-a45c-40e0-964d-5e536e2ada61
      max work queue size: 0
      max work queue data size: 1 MB
      flowfile expiration: 60 sec
      queue prioritizer class: org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer

Remote Processing Groups:
    - name: NiFi Flow
      comment:
      url: https://localhost:8090/nifi
      timeout: 30 secs
      yield period: 10 sec
      Input Ports:
          - id: 8644cbcc-a45c-40e0-964d-5e536e2ada61
            name: tailed log
            comments:
            max concurrent tasks: 1
            use compression: false

Provenance Reporting:
    comment:
    scheduling strategy: TIMER_DRIVEN
    scheduling period: 30 sec
    destination url: https://localhost:8090/
    port name: provenance
    originating url: http://${hostname(true)}:8081/nifi
    use compression: true
    timeout: 30 secs
    batch size: 1000
```


``` yaml
Flow Controller:
    name: MiNiFi Flow
    comment:

Core Properties:
    flow controller graceful shutdown period: 10 sec
    flow service write delay interval: 500 ms
    administrative yield duration: 30 sec
    bored yield duration: 10 millis
    max concurrent threads: 1

FlowFile Repository:
    partitions: 256
    checkpoint interval: 2 mins
    always sync: false
    Swap:
        threshold: 20000
        in period: 5 sec
        in threads: 1
        out period: 5 sec
        out threads: 4

Content Repository:
    content claim max appendable size: 10 MB
    content claim max flow files: 100
    always sync: false

Component Status Repository:
    buffer size: 1440
    snapshot frequency: 1 min

Security Properties:
    keystore: /tmp/ssl/localhost-ks.jks
    keystore type: JKS
    keystore password: localtest
    key password: localtest
    truststore: /tmp/ssl/localhost-ts.jks
    truststore type: JKS
    truststore password: localtest
    ssl protocol: TLS
    Sensitive Props:
        key:
        algorithm: PBEWITHMD5AND256BITAES-CBC-OPENSSL
        provider: BC

Processors:
    - name: TailAppLog
      class: org.apache.nifi.processors.standard.TailFile
      max concurrent tasks: 1
      scheduling strategy: TIMER_DRIVEN
      scheduling period: 10 sec
      penalization period: 30 sec
      yield period: 1 sec
      run duration nanos: 0
      auto-terminated relationships list:
      Properties:
          File to Tail: logs/minifi-app.log
          Rolling Filename Pattern: minifi-app*
          Initial Start Position: Beginning of File
    - name: SplitIntoSingleLines
      class: org.apache.nifi.processors.standard.SplitText
      max concurrent tasks: 1
      scheduling strategy: TIMER_DRIVEN
      scheduling period: 0 sec
      penalization period: 30 sec
      yield period: 1 sec
      run duration nanos: 0
      auto-terminated relationships list:
          - failure
          - original
      Properties:
          Line Split Count: 1
          Header Line Count: 0
          Remove Trailing Newlines: true
    - name: RouteErrors
      class: org.apache.nifi.processors.standard.RouteText
      max concurrent tasks: 1
      scheduling strategy: TIMER_DRIVEN
      scheduling period: 0 sec
      penalization period: 30 sec
      yield period: 1 sec
      run duration nanos: 0
      auto-terminated relationships list:
          - unmatched
          - original
      Properties:
          Routing Strategy: Route to 'matched' if line matches all conditions
          Matching Strategy: Contains
          Character Set: UTF-8
          Ignore Leading/Trailing Whitespace: true
          Ignore Case: true
          Grouping Regular Expression:
          WALFFR: WriteAheadFlowFileRepository
    - name: PutFile
      class: org.apache.nifi.processors.standard.PutFile
      max concurrent tasks: 1
      scheduling strategy: TIMER_DRIVEN
      scheduling period: 0 sec
      penalization period: 30 sec
      yield period: 1 sec
      run duration nanos: 0
      auto-terminated relationships list:
          - failure
          - success
      Properties:
          Directory: ./
          Conflict Resolution Strategy: replace
          Create Missing Directories: true
          Maximum File Count:
          Last Modified Time:
          Permissions:
          Owner:
          Group:

Connections:
    - name: TailToSplit
      source name: TailAppLog
      source relationship name: success
      destination name: SplitIntoSingleLines
      max work queue size: 0
      max work queue data size: 1 MB
      flowfile expiration: 60 sec
      queue prioritizer class: org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer
    - name: SplitToRoute
      source name: SplitIntoSingleLines
      source relationship name: splits
      destination name: RouteErrors
      max work queue size: 0
      max work queue data size: 1 MB
      flowfile expiration: 60 sec
      queue prioritizer class: org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer
    - name: RouteToS2S
      source name: RouteErrors
      source relationship name: matched
      destination name: PutFile
      max work queue size: 0
      max work queue data size: 1 MB
      flowfile expiration: 60 sec
      queue prioritizer class: org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer

Provenance Reporting:
    comment:
    scheduling strategy: TIMER_DRIVEN
    scheduling period: 30 sec
    destination url: https://localhost:8080/
    port name: provenance
    originating url: http://${hostname(true)}:8081/nifi
    use compression: true
    timeout: 30 secs
    batch size: 1000
```