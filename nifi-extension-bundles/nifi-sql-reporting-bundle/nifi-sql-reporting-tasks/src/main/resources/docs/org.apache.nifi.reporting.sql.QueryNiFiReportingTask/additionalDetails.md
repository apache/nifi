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

# QueryNiFiReportingTask

## Summary

This reporting task can be used to issue SQL queries against various NiFi metrics information, modeled as tables, and
transmit the query results to some specified destination. The query may make use of the CONNECTION\_STATUS,
PROCESSOR\_STATUS, BULLETINS, PROCESS\_GROUP\_STATUS, JVM\_METRICS, CONNECTION\_STATUS\_PREDICTIONS, or PROVENANCE
tables, and can use any functions or capabilities provided by [Apache Calcite](https://calcite.apache.org/), including
JOINs, aggregate functions, etc.

The results are transmitted to the destination using the configured Record Sink service, such as
SiteToSiteReportingRecordSink (for sending via the Site-to-Site protocol) or DatabaseRecordSink (for sending the query
result rows to a relational database).

The reporting task can uniquely handle items from the bulletin and provenance repositories. This means that an item will
only be processed once when the query is set to unique. The query can be set to unique by defining a time window with
special sql placeholders ($bulletinStartTime, $bulletinEndTime, $provenanceStartTime, $provenanceEndTime) that the
reporting task will evaluate runtime. See the SQL Query Examples section.

## Table Definitions

Below is a list of definitions for all the "tables" supported by this reporting task. Note that these are not
persistent/materialized tables, rather they are non-materialized views for which the sources are re-queried at every
execution. This means that a query executed twice may return different results, for example if new status information is
available, or in the case of JVM\_METRICS (for example), a new snapshot of the JVM at query-time.

### CONNECTION\_STATUS

| Column                        | Data Type |
|-------------------------------|-----------|
| id                            | String    |
| groupId                       | String    |
| name                          | String    |
| sourceId                      | String    |
| sourceName                    | String    |
| destinationId                 | String    |
| destinationName               | String    |
| backPressureDataSizeThreshold | String    |
| backPressureBytesThreshold    | long      |
| backPressureObjectThreshold   | long      |
| isBackPressureEnabled         | boolean   |
| inputCount                    | int       |
| inputBytes                    | long      |
| queuedCount                   | int       |
| queuedBytes                   | long      |
| outputCount                   | int       |
| outputBytes                   | long      |
| maxQueuedCount                | int       |
| maxQueuedBytes                | long      |

### PROCESSOR\_STATUS

| Column                 | Data Type |
|------------------------|-----------|
| id                     | String    |
| groupId                | String    |
| name                   | String    |
| processorType          | String    |
| averageLineageDuration | long      |
| bytesRead              | long      |
| bytesWritten           | long      |
| bytesReceived          | long      |
| bytesSent              | long      |
| flowFilesRemoved       | int       |
| flowFilesReceived      | int       |
| flowFilesSent          | int       |
| inputCount             | int       |
| inputBytes             | long      |
| outputCount            | int       |
| outputBytes            | long      |
| activeThreadCount      | int       |
| terminatedThreadCount  | int       |
| invocations            | int       |
| processingNanos        | long      |
| runStatus              | String    |
| executionNode          | String    |

### BULLETINS

| Column               | Data Type |
|----------------------|-----------|
| bulletinId           | long      |
| bulletinCategory     | String    |
| bulletinGroupId      | String    |
| bulletinGroupName    | String    |
| bulletinGroupPath    | String    |
| bulletinLevel        | String    |
| bulletinMessage      | String    |
| bulletinNodeAddress  | String    |
| bulletinNodeId       | String    |
| bulletinSourceId     | String    |
| bulletinSourceName   | String    |
| bulletinSourceType   | String    |
| bulletinTimestamp    | Date      |
| bulletinFlowFileUuid | String    |

### PROCESS\_GROUP\_STATUS

| Column                | Data Type |
|-----------------------|-----------|
| id                    | String    |
| groupId               | String    |
| name                  | String    |
| bytesRead             | long      |
| bytesWritten          | long      |
| bytesReceived         | long      |
| bytesSent             | long      |
| bytesTransferred      | long      |
| flowFilesReceived     | int       |
| flowFilesSent         | int       |
| flowFilesTransferred  | int       |
| inputContentSize      | long      |
| inputCount            | int       |
| outputContentSize     | long      |
| outputCount           | int       |
| queuedContentSize     | long      |
| activeThreadCount     | int       |
| terminatedThreadCount | int       |
| queuedCount           | int       |
| versionedFlowState    | String    |
| processingNanos       | long      |

### JVM\_METRICS

The JVM\_METRICS table has dynamic columns in the sense that the "garbage collector runs" and "garbage collector time
columns" appear for each Java garbage collector in the JVM.  
The column names end with the name of the garbage collector substituted for the `<garbage_collector_name>` expression
below:

| Column                                    | Data Type |
|-------------------------------------------|-----------|
| jvm\_daemon\_thread\_count                | int       |
| jvm\_thread\_count                        | int       |
| jvm\_thread\_states\_blocked              | int       |
| jvm\_thread\_states\_runnable             | int       |
| jvm\_thread\_states\_terminated           | int       |
| jvm\_thread\_states\_timed\_waiting       | int       |
| jvm\_uptime                               | long      |
| jvm\_head\_used                           | double    |
| jvm\_heap\_usage                          | double    |
| jvm\_non\_heap\_usage                     | double    |
| jvm\_file\_descriptor\_usage              | double    |
| jvm\_gc\_runs\_`<garbage_collector_name>` | long      |
| jvm\_gc\_time\_`<garbage_collector_name>` | long      |

### CONNECTION\_STATUS\_PREDICTIONS

| Column                                 | Data Type |
|----------------------------------------|-----------|
| connectionId                           | String    |
| predictedQueuedBytes                   | long      |
| predictedQueuedCount                   | int       |
| predictedPercentBytes                  | int       |
| predictedPercentCount                  | int       |
| predictedTimeToBytesBackpressureMillis | long      |
| predictedTimeToCountBackpressureMillis | long      |
| predictionIntervalMillis               | long      |

### PROVENANCE

| Column              | Data Type          |
|---------------------|--------------------|
| eventId             | long               |
| eventType           | String             |
| timestampMillis     | long               |
| durationMillis      | long               |
| lineageStart        | long               |
| details             | String             |
| componentId         | String             |
| componentName       | String             |
| componentType       | String             |
| processGroupId      | String             |
| processGroupName    | String             |
| entityId            | String             |
| entityType          | String             |
| entitySize          | long               |
| previousEntitySize  | long               |
| updatedAttributes   | Map<String,String> |
| previousAttributes  | Map<String,String> |
| contentPath         | String             |
| previousContentPath | String             |
| parentIds           | Array<String>      |
| childIds            | Array<String>      |
| transitUri          | String             |
| remoteIdentifier    | String             |
| alternateIdentifier | String             |

### FLOW\_CONFIG\_HISTORY

| Column                        | Data Type |
|-------------------------------|-----------|
| actionId                      | int       |
| actionTimestamp               | long      |
| actionUserIdentity            | String    |
| actionSourceId                | String    |
| actionSourceName              | String    |
| actionSourceType              | String    |
| actionOperation               | String    |
| configureDetailsName          | String    |
| configureDetailsPreviousValue | String    |
| configureDetailsValue         | String    |
| connectionSourceId            | String    |
| connectionSourceName          | String    |
| connectionSourceType          | String    |
| connectionDestinationId       | String    |
| connectionDestinationName     | String    |
| connectionDestinationType     | String    |
| connectionRelationship        | String    |
| moveGroup                     | String    |
| moveGroupId                   | String    |
| movePreviousGroup             | String    |
| movePreviousGroupId           | String    |
| purgeEndDate                  | long      |

## SQL Query Examples

**Example:** Select all fields from the `CONNECTION_STATUS` table:

```sql
SELECT * FROM CONNECTION_STATUS
```

**Example:** Select connection IDs where time-to-backpressure (based on queue count) is less than 5 minutes:

```sql
SELECT connectionId FROM CONNECTION_STATUS_PREDICTIONS WHERE predictedTimeToCountBackpressureMillis < 300000
```

**Example:** Get the unique bulletin categories associated with errors:

```sql
SELECT DISTINCT(bulletinCategory) FROM BULLETINS WHERE bulletinLevel = "ERROR"
```

**Example:** Select all fields from the `BULLETINS` table with time window:

```sql
SELECT * from BULLETINS WHERE bulletinTimestamp > $bulletinStartTime AND bulletinTimestamp <= $bulletinEndTime
```

**Example:** Select all fields from the `PROVENANCE` table with time window:

```sql
SELECT * from PROVENANCE where timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime
```

**Example:** Select connection-related fields from the `FLOW_CONFIG_HISTORY` table:

```sql
SELECT connectionSourceName, connectionDestinationName, connectionRelationship
from FLOW_CONFIG_HISTORY
```
