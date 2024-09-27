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

# ListenFTP

## Usage Description

By starting the processor, an FTP server is started that listens for incoming connections on the specified port. Each
file copied to this FTP server gets converted into a FlowFile and transferred to the next processor via the ListenFTP
processor's 'success' relationship.

Before starting the processor, the following properties can be set:

* **Bind Address:** if not set, the FTP server binds to all network interfaces of the host machine (this is the
  default). If set to a valid address, the server is only available on that specific address.
* **Listening Port:** the port on which the server listens for incoming connections. Root privileges are required on
  Linux to be able to use port numbers below 1024.
* **Username and Password:** Either both of them need to be set, or none of them. If set, the FTP server only allows
  users to log in with the username-password pair specified in these properties. If the Username and Password properties
  are left blank, the FTP server allows anonymous connections, meaning that the client can connect to the FTP server by
  providing 'anonymous' as username, and leaving the password field blank. Setting empty string as the value of these
  properties is not permitted, and doing so results in the processor becoming invalid.
* **SSL Context Service:** a Controller Service can optionally be specified that provides the ability to configure
  keystore and/or truststore properties. When not specified, the FTP server does not use encryption. By specifying an
  SSL Context Service, the FTP server started by this processor is set to use Transport Layer Security (TLS) over FTP (
  FTPS).  
  If an SSL Context Service is selected, then a keystore file must also be specified in the SSL Context Service. Without
  a keystore file, the processor cannot be started successfully.  
  Specifying a truststore file is optional. If a truststore file is specified, client authentication is required (the
  client needs to send a certificate to the server).  
  Regardless of the selected TLS protocol, the highest available protocol is used for the connection. For example if
  NiFi is running on Java 11 and TLSv1.2 is selected in the controller service as the preferred TLS protocol, TLSv1.3
  will be used (regardless of TLSv1.2 being selected) because Java 11 supports TLSv1.3.

After starting the processor and connecting to the FTP server, an empty root directory is visible in the client
application. Folders can be created in and deleted from the root directory and any of its subdirectories. Files can be
uploaded to any directory. **Uploaded files do not show in the content list of directories**, since files are not
actually stored on this FTP server, but converted into FlowFiles and transferred to the next processor via the 'success'
relationship. It is not possible to download or delete files like on a regular FTP server.  
All the folders (including the root directory) are virtual directories, meaning that they only exist in memory and do
not get created in the file system of the host machine. Also, these directories are not persisted: by restarting the
processor all the directories (except for the root directory) get removed. Uploaded files do not get removed by
restarting the processor, since they are not stored on the FTP server, but transferred to the next processor as
FlowFiles.  
When a file named for example _text01.txt_ is uploaded to the target folder _/MyDirectory/MySubdirectory_, a FlowFile
gets created. The content of the FlowFile is the same as the content of _text01.txt_, the 'filename' attribute of the
FlowFile contains the name of the original file (_text01.txt_) and the 'path' attribute of the flowfile contains the
path where the file was uploaded (_/MyDirectory/MySubdirectory/_).

The list of the FTP commands that are supported by the FTP server is available by starting the processor and issuing
the 'HELP' command to the server from an FTP client application.