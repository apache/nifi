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

# AzureLogAnalyticsReportingTask

## AzureLogAnalyticsReportingTask

This ReportingTask sends the following metrics to Azure Log Analytics workspace:

*   FlowFilesReceivedLast5Minutes
*   BytesReceivedLast5Minutes
*   FlowFilesSentLast5Minutes
*   BytesSentLast5Minutes
*   FlowFilesQueued
*   BytesQueued
*   BytesReadLast5Minutes
*   BytesWrittenLast5Minutes
*   ActiveThreads
*   TotalTaskDurationSeconds
*   jvm.uptime
*   jvm.heap\_used
*   jvm.heap\_usage
*   jvm.non\_heap\_usage
*   jvm.thread\_states.runnable
*   jvm.thread\_states.blocked
*   jvm.thread\_states.timed\_waiting
*   jvm.thread\_states.terminated
*   jvm.thread\_count
*   jvm.daemon\_thread\_count
*   jvm.file\_descriptor\_usage
*   jvm.gc.runs
*   jvm.gc.time