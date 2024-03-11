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


# nifi-py4j-bundle module

The NiFi Py4J Bundle provides a linkage between NiFi's Java Process and Python.
Py4J is the library used in order to launch an RPG server that can be used to communicate between
the Java and Python Processes.

See the NiFi Python Developer's Guide for more information about how to build Processors in Python.


# Debugging

There are times when it's helpful to enable remote debugging of the Python code. Because NiFi is responsible
for launching the Python process, how to enable this may not be as straight-forward as when launching a Python
process yourself. However, NiFi can be told to enable remote debugging when launching the Python process.

The manner in which you connect to the Python Process differs by IDE. Here, we will examine how to use
VSCode's DebugPy.

## Debugging Framework

The method for debugging the framework and debugging Processors is different. Typically, when performing debugging
on the Framework itself, it is easiest to have NiFi enable a DebugPy listener when launching the Python process
that hosts the Controller.

To enable remote debugging, NiFi will use `pip` to install the `debugpy` module into the environment used by the main Python process.
This process is used to discover available Processors and to create Processors. It is not used by Processors themselves.


### Listen for Incoming Connections (Controller)
The following properties may be added to nifi.properties in order to enable remote debugging
of the Controller process:

`nifi.python.controller.debugpy.enabled` : Indicates whether DebugPy should be used when launching the Controller.
Defaults to `false`. If set to `true`, the Python process that is responsible for discovering and creating Processors
will be launched using DebugPy.

`nifi.python.controller.debugpy.port` : The local port to use. Defaults to `5678`.

`nifi.python.controller.debugpy.host` : The hostname to listen on. Defaults to `localhost`.

`nifi.python.controller.debugpy.logs.directory` : The directory to write DebugPy logs to. Defaults to `./logs`


Note that these properties do not exist in the nifi.properties by default.
This is intentional and is due to the fact that during any normal operations, this should not be used.
This should be used only by developers wanting to debug the NiFi application itself.


### Connecting to the Python Process

It is important, however, to note the host and port that the debugger is using.
When establishing a connection to the remote debugger, the VSCode may be configured with both the local
directory to use for Python source files, as well as the remote debugger.

Generally, the local directory should point to `${NIFI_SOURCE_DIRECTORY}/nifi-nar-bundles/nifi-py4j-bundle/nifi-python-framework/src/main/python/framework`.
The remote directory, which defaults to `.` should be specified as `./python/framework`.


## Debugging Processors

It is also important to enable remote debugging for Processors. We expect Processor developers to be able
to do this, not just those who are maintaining the NiFi codebase. As a result, instructions for enabling
remote debugging of Processors has been added to the NiFi Python Developer's Guide.
