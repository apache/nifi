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

# WindowsEventLogReader

## Description:

This controller service is used to parse Windows Event Log events in the form of XML input (possibly from
ConsumeWindowsEventLog or ParseEvtx).

## Input XML Example:

```xml
<Event xmlns="https://schemas.microsoft.com/win/2004/08/events/event">
    <System>
        <Provider Name="Service Control Manager" Guid="{555908d1-a6d7-4695-8e1e-26931d2012f4}"
                  EventSourceName="Service Control Manager"/>
        <EventID Qualifiers="16384">7036</EventID>
        <Version>0</Version>
        <Level>4</Level>
        <Task>0</Task>
        <Opcode>0</Opcode>
        <Keywords>0x8080000000000000</Keywords>
        <TimeCreated SystemTime="2016-06-10T22:28:53.905233700Z"/>
        <EventRecordID>34153</EventRecordID>
        <Correlation/>
        <Execution ProcessID="684" ThreadID="3504"/>
        <Channel>System</Channel>
        <Computer>WIN-O05CNUCF16M.hdf.local</Computer>
        <Security/>
    </System>
    <EventData>
        <Data Name="param1">Smart Card Device Enumeration Service</Data>
        <Data>param2</Data>
        <Binary>5300630044006500760069006300650045006E0075006D002F0034000000</Binary>
    </EventData>
</Event>
```

## Output example (using ConvertRecord with JsonRecordSetWriter

```json
[
  {
    "System": {
      "Provider": {
        "Guid": "{555908d1-a6d7-4695-8e1e-26931d2012f4}",
        "Name": "Service Control Manager"
      },
      "EventID": 7036,
      "Version": 0,
      "Level": 4,
      "Task": 0,
      "Opcode": 0,
      "Keywords": "0x8080000000000000",
      "TimeCreated": {
        "SystemTime": "2016-06-10T22:28:53.905233700Z"
      },
      "EventRecordID": 34153,
      "Correlation": null,
      "Execution": {
        "ThreadID": 3504,
        "ProcessID": 684
      },
      "Channel": "System",
      "Computer": "WIN-O05CNUCF16M.hdf.local",
      "Security": null
    },
    "EventData": {
      "param1": "Smart Card Device Enumeration Service",
      "param2": "5300630044006500760069006300650045006E0075006D002F0034000000"
    }
  }
]
```