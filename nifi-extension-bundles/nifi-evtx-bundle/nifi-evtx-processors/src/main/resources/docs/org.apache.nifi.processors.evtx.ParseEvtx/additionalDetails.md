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

# ParseEvtx

## Description:

This processor is used to parse Windows event logs in the binary evtx format. The input flow files' content should be
evtx files. The processor has 4 outputs:

* The original unmodified FlowFile
* The XML resulting from parsing at the configured granularity
* Failed parsing with partial output
* Malformed chunk in binary form

## Output XML Example:

```xml
<?xml version="1.0"?>
<Events>
    <Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
        <System>
            <Provider Name="Service Control Manager" Guid="{555908d1-a6d7-4695-8e1e-26931d2012f4}" Ev
                      entSourceName="Service Control Manager"/>
            <EventID Qualifiers="16384">7036</EventID>
            <Version>0</Version>
            <Level>4</Level>
            <Task>0</Task>
            <Opcode>0</Opcode>
            <Keywords>0x8080000000000000</Keywords>
            <TimeCreated SystemTime="2016-01-08 16:49:47.518"/>
            <EventRecordID>780</EventRecordID>
            <Correlation ActivityID="" RelatedActivityID=""/>
            <Execution ProcessID="480" ThreadID="596"/>
            <Channel>System</Channel>
            <Computer>win7-pro-vm</Computer>
            <Security UserID=""/>
        </System>
        <EventData>
            <Data Name="param1">Workstation</Data>
            <Data Name="param2">running</Data>
            <Binary>TABhAG4AbQBhAG4AVwBvAHIAawBzAHQAYQB0AGkAbwBuAC8ANAAAAA==</Binary>
        </EventData>
    </Event>
    <Event xmlns="http://schemas.microsoft.com/win/2004/08/events/event">
        <System>
            <Provider Name="Service Control Manager" Guid="{555908d1-a6d7-4695-8e1e-26931d2012f4}"
                      EventSourceName="Service Control Manager"/>
            <EventID Qualifiers="16384">7036</EventID>
            <Version>0</Version>
            <Level>4</Level>
            <Task>0</Task>
            <Opcode>0</Opcode>
            <Keywords>0x8080000000000000</Keywords>
            <TimeCreated SystemTime="2016-01-08 16:49:47.535"/>
            <EventRecordID>781</EventRecordID>
            <Correlation ActivityID="" RelatedActivityID=""/>
            <Execution ProcessID="480" ThreadID="576"/>
            <Channel>System</Channel>
            <Computer>win7-pro-vm</Computer>
            <Security UserID=""/>
        </System>
        <EventData>
            <Data Name="param1">Cryptographic Services</Data>
            <Data Name="param2">running</Data>
            <Binary>QwByAHkAcAB0AFMAdgBjAC8ANAAAAA==</Binary>
        </EventData>
    </Event>
</Events>
```
