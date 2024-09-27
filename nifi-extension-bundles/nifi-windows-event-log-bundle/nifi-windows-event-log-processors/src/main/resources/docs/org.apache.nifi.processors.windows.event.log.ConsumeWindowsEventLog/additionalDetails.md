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

# ConsumeWindowsEventLog

## Description:

This processor is used listen to Windows Event Log events. It has a success output that will contain an XML
representation of the event.

## Output XML Example:

```xml
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
        <Data Name="param2">running</Data>
        <Binary>5300630044006500760069006300650045006E0075006D002F0034000000</Binary>
    </EventData>
</Event>
```

## Permissions:

Your Windows User must have permissions to read the given Event Log. This can be achieved through the following steps (
Windows 2008 and newer):

1. Open a command prompt as your user. Enter the command: wmic useraccount get name,sid
2. Note the SID of the user or group you'd like to allow to read a given channel
3. Open a command prompt as Administrator. enter the command: wevtutil gl CHANNEL\_NAME
4. Take the channelAccess Attribute starting with O:BAG, copy it into a text editor, and add (
   A;;0x1;;;YOUR\_SID\_FROM\_BEFORE) to the end
5. Take that text and run the following command in your admin prompt (see below for example): wevtutil sl CHANNEL\_NAME
   /ca:TEXT\_FROM\_PREVIOUS\_STEP

The following command is the exact one I used to add read access to the Security log for my user. (You can see all the
possible channels with: wevtutil el):

```
wevtutil sl Security /ca:O:BAG:SYD:(A;;0xf0005;;;SY)(A;;0x5;;;BA)(A;;0x1;;;S-1-5-32-573)(A;;0x1;;;S-1-5-21-3589080292-3448680409-2446571098-1001)
```

These steps were adapted
from [this guide.](https://blogs.technet.microsoft.com/janelewis/2010/04/30/giving-non-administrators-permission-to-read-event-logs-windows-2003-and-windows-2008/)