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

# ControllerStatusReportingTask

## Description:

Reporting Task that creates a log message for each Processor and each Connection in the flow. For Processors, the
following information is included (sorted by descending Processing Timing):

* Processor Name
* Processor ID
* Processor Type
* Run Status
* Flow Files In (5 mins)
* FlowFiles Out (5 mins)
* Bytes Read from Disk (5 mins)
* Bytes Written to Disk (5 mins)
* Number of Tasks Completed (5 mins)
* Processing Time (5 mins)

For Connections, the following information is included (sorted by descending size of queued FlowFiles):

* Connection Name
* Connection ID
* Source Component Name
* Destination Component Name
* Flow Files In (5 mins)
* FlowFiles Out (5 mins)
* FlowFiles Queued

It may be convenient to redirect the logging output of this ReportingTask to a separate log file than the typical
application log. This can be accomplished by modified the logback.xml file in the NiFi conf/ directory such that a
logger with the name `org.apache.nifi.controller.ControllerStatusReportingTask` is configured to write to a separate
log.

Additionally, it may be convenient to disable logging for Processors or for Connections or to split them into separate
log files. This can be accomplished by using the loggers named
`org.apache.nifi.controller.ControllerStatusReportingTask.Processors` and
`org.apache.nifi.controller.ControllerStatusReportingTask.Connections`, respectively.