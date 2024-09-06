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

# TailFile

### Introduction

This processor offers a powerful capability, allowing the user to periodically look at a file that is actively being
written to by another process. When the file changes, the new lines are ingested. This Processor assumes that data in
the file is textual.

Tailing a file from a filesystem is a seemingly simple but notoriously difficult task. This is because we are
periodically checking the contents of a file that is being written to. The file may be constantly changing, or it may
rarely change. The file may be "rolled over" (i.e., renamed) and it's important that even after restarting the
application (NiFi, in this case), we are able to pick up where we left off. Other additional complexities also come into
play. For example, NFS mounted drives may indicate that data is readable but then return NUL bytes (Unicode 0) when
attempting to read, as the actual bytes are not yet known (see the <Reread when NUL encountered> property), and file
systems have different timestamp granularities.

This Processor is designed to handle all of these different cases. This can lead to slightly more complex configuration,
but this document should provide you with all you need to get started!

### Modes

This processor is used to tail a file or multiple files, depending on the configured mode. The mode to choose depends on
the logging pattern followed by the file(s) to tail. In any case, if there is a rolling pattern, the rolling files must
be plain text files (compression is not supported at the moment).

* **Single file**: the processor will tail the file with the path given in 'File(s) to tail' property.
* **Multiple files**: the processor will look for files into the 'Base directory'. It will look for file recursively
  according to the 'Recursive lookup' property and will tail all the files matching the regular expression provided in
  the 'File(s) to tail' property.

### Rolling filename pattern

In case the 'Rolling filename pattern' property is used, when the processor detects that the file to tail has rolled
over, the processor will look for possible missing messages in the rolled file. To do so, the processor will use the
pattern to find the rolling files in the same directory as the file to tail.

In order to keep this property available in the 'Multiple files' mode when multiples files to tail are in the same
directory, it is possible to use the ${filename} tag to reference the name (without extension) of the file to tail. For
example, if we have:

`/my/path/directory/my-app.log.1   /my/path/directory/my-app.log   /my/path/directory/application.log.1   /my/path/directory/application.log`

the 'rolling filename pattern' would be _${filename}.log.\*_.

### Descriptions for different modes and strategies

The '**Single file**' mode assumes that the file to tail has always the same name even if there is a rolling pattern.
Example:

`/my/path/directory/my-app.log.2   /my/path/directory/my-app.log.1   /my/path/directory/my-app.log`

and new log messages are always appended in my-app.log file.

In case recursivity is set to 'true'. The regular expression for the files to tail must embrace the possible
intermediate directories between the base directory and the files to tail. Example:

`/my/path/directory1/my-app1.log   /my/path/directory2/my-app2.log   /my/path/directory3/my-app3.log`

`Base directory: /my/path   Files to tail: directory[1-3]/my-app[1-3].log   Recursivity: true`

If the processor is configured with '**Multiple files**' mode, two additional properties are relevant:

* **Lookup frequency**: specifies the minimum duration the processor will wait before listing again the files to tail.
* **Maximum age**: specifies the necessary minimum duration to consider that no new messages will be appended in a file
  regarding its last modification date. If the amount of time that has elapsed since the file was modified is larger
  than this period of time, the file will not be tailed. For example, if a file was modified 24 hours ago and this
  property is set to 12 hours, the file will not be tailed. But if this property is set to 36 hours, then the file will
  continue to be tailed.

It is necessary to pay attention to 'Lookup frequency' and 'Maximum age' properties, as well as the frequency at which
the processor is triggered, in order to achieve high performance. It is recommended to keep 'Maximum age' > 'Lookup
frequency' > processor scheduling frequency to avoid missing data. It also recommended not to set 'Maximum Age' too low
because if messages are appended in a file after this file has been considered "too old", all the messages in the file
may be read again, leading to data duplication.

If the processor is configured with '**Multiple files**' mode, the 'Rolling filename pattern' property must be specific
enough to ensure that only the rolling files will be listed and not other currently tailed files in the same directory (
this can be achieved using ${filename} tag).

### Handling Multi-Line Messages

Most of the time, when we tail a file, we are happy to receive data periodically, however it was written to the file.
There are scenarios, though, where we may have data written in such a way that multiple lines need to be retained
together. Take, for example, the following lines of text that might be found in a log file:

```
2021-07-09 14:12:19,731 INFO [main] org.apache.nifi.NiFi Launching NiFi... 
2021-07-09 14:12:19,915 INFO [main] o.a.n.p.AbstractBootstrapPropertiesLoader Determined default application properties path to be '/Users/mpayne/devel/nifi/nifi-assembly/target/nifi-1.14.0-SNAPSHOT-bin/nifi-1.14.0-SNAPSHOT/./conf/nifi.properties' 
2021-07-09 14:12:19,919 INFO [main] o.a.nifi.properties.NiFiPropertiesLoader Loaded 199 properties from /Users/mpayne/devel/nifi/nifi-assembly/target/nifi-1.14.0-SNAPSHOT-bin/nifi-1.14.0-SNAPSHOT/./conf/nifi.properties 
2021-07-09 14:12:19,925 WARN Line 1 of Log Message 			Line 2: This is an important warning. 			Line 3: Please do not ignore this warning. 			Line 4: These lines of text make sense only in the context of the original message. 
2021-07-09 14:12:19,941 INFO [main] Final message in log file
```

In this case, we may want to ensure that the log lines are not ingested in such a way that our multi-line log message is
not broken up into Lines 1 and 2 in one FlowFile and Lines 3 and 4 in another. To accomplish this, the Processor exposes
the <Line Start Pattern> property. If we set this Property to a value of `\d{4}-\d{2}-\d{2}`, then we are telling the
Processor that each message should begin with 4 digits, followed by a dash, followed by 2 digits, a dash, and 2 digits.
I.e., we are telling it that each message begins with a timestamp in yyyy-MM-dd format. Because of this, even if the
Processor runs and sees only Lines 1 and 2 of our multiline log message, it will not ingest the data yet. It will wait
until it sees the next message, which starts with a timestamp.

Note that, because of this, the last message that the Processor will encounter in the above situation is the "Final
message in log file" line. At this point, the Processor does not know whether the next line of text it encounters will
be part of this line or a new message. As such, it will not ingest this data. It will wait until either another message
is encountered (that matches our regex) or until the file is rolled over (renamed). Because of this, there may be some
delay in ingesting the last message in the file, if the process that writes to the file just stops writing at this
point.

Additionally, we run the chance of the Regular Expression not matching the data in the file. This could result in
buffering all the file's content, which could cause NiFi to run out of memory. To avoid this, the <Max Buffer Size>
property limits the amount of data that can be buffered. If this amount of data is buffered, it will be flushed to the
FlowFile, even if another message hasn't been encountered.