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

# ExecuteStreamCommand

## Description

The ExecuteStreamCommand processor provides a flexible way to integrate external commands and scripts into NiFi data
flows. ExecuteStreamCommand can pass the incoming FlowFile's content to the command that it executes similarly how
piping works.

## Configuration options

### Working Directory

If not specified, NiFi root will be the default working directory.

### Configuring command arguments

The ExecuteStreamCommand processor provides two ways to specify command arguments: using Dynamic Properties and the
Command Arguments field.

#### Command Arguments field

This is the default. If there are multiple arguments, they need to be separated by a character specified in the Argument
Delimiter field. When needed, '-' and '--' can be provided, but in these cases Argument Delimiter should be a different
character.

Consider that we want to list all files in a directory which is different from the working directory:

| Command Path | Command Arguments  |
|--------------|--------------------|
| ls           | \-lah;/path/to/dir |

**NOTE:** the command should be on `$PATH` or it should be in the working directory, otherwise path also should be
specified.

#### Dynamic Properties

Arguments can be specified with Dynamic Properties. Dynamic Properties with the pattern of '
command.argument.<commandIndex>' will be appended to the command in ascending order.

The above example with dynamic properties would look like this:

| Property Name      | Property Value |
|--------------------|----------------|
| command.argument.0 | \-lah          |
| command.argument.1 | /path/to/dir   |

### Configuring environment variables

In addition to specifying command arguments using the Command Argument field or Dynamic Properties, users can also use
environment variables with the ExecuteStreamCommand processor. Environment variables are a set of key-value pairs that
can be accessed by processes running on the system. ExecuteStreamCommand will treat every Dynamic Property as an
environment variable that doesn't match the pattern 'command.argument.<commandIndex>'.

Consider that we want to execute a Maven command with the processor. If there are multiple Java versions installed on
the system, you can specify which version will be used by setting the `JAVA_HOME` environment variable. The output
FlowFile will looke like this if we run `mvn` command with `--version` argument:

```
Apache Maven 3.8.6 (84538c9988a25aec085021c365c560670ad80f63) 
Maven home: /path/to/maven/home Java version: 11.0.18, vendor: Eclipse Adoptium, runtime: /path/to/default/java/home 
Default locale: en_US, platform encoding: UTF-8 OS name: "mac os x", version: "13.1", arch: "x86_64", family: "mac"
```

| Property Name | Property Value            |
|---------------|---------------------------|
| JAVA\_HOME    | path/to/another/java/home |

After specifying the `JAVA_HOME` property, you can notice that maven is using a different runtime:

```
Apache Maven 3.8.6 (84538c9988a25aec085021c365c560670ad80f63) 
Maven home: /path/to/maven/home Java version: 11.0.18, vendor: Eclipse Adoptium, runtime: /path/to/another/java/home 
Default locale: en_US, platform encoding: UTF-8 OS name: "mac os x", version: "13.1", arch: "x86_64", family: "mac"
```

### Streaming input to the command

ExecuteStreamCommand passes the incoming FlowFile's content to the command that it executes similarly how piping works.
It is possible to disable this behavior with the Ignore STDIN property. In the above examples we didn't use the incoming
FlowFile's content, so in this case we could leverage this property for additional performance gain.

To utilize the streaming capability, consider that we want to use `grep` on the FlowFile. Let's presume that we need to
list all `POST` requests from an Apache HTTPD log:

```
127.0.0.1 - - [03/May/2023:13:54:26 +0000] "GET /example-page HTTP/1.1" 200 4825 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0" 
127.0.0.1 - - [03/May/2023:14:05:32 +0000] "POST /submit-form HTTP/1.1" 302 0 "http://localhost/example-page" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0" 
127.0.0.1 - - [03/May/2023:14:10:48 +0000] "GET /image.jpg HTTP/1.1" 200 35785 "http://localhost/example-page" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0" 
127.0.0.1 - - [03/May/2023:14:20:15 +0000] "GET /example-page HTTP/1.1" 200 4825 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0" 
127.0.0.1 - - [03/May/2023:14:30:42 +0000] "GET /example-page HTTP/1.1" 200 4825 "-" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"
```

Processor configuration:

| Working Directory | Command Path | Command Arguments Strategy | Command Arguments | Argument Delimiter | Ignore STDIN | Output Destination Attribute | Max Attribute Length |
|-------------------|--------------|----------------------------|-------------------|--------------------|--------------|------------------------------|----------------------|
|                   | grep         | Command Arguments Property | POST              | ;                  | false        |                              | 256                  |

With this the emitted FlowFile on the "output stream" relationship should be:

```
127.0.0.1 - - [03/May/2023:14:05:32 +0000] "POST /submit-form HTTP/1.1" 302 0 "http://localhost/example-page" "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0"
```