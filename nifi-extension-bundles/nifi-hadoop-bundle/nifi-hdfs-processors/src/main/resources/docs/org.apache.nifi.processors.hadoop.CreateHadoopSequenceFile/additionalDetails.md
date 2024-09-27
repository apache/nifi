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

# CreateHadoopSequenceFile

## Description

This processor is used to create a Hadoop Sequence File, which essentially is a file of key/value pairs. The key will be
a file name and the value will be the flow file content. The processor will take either a merged (a.k.a. packaged) flow
file or a singular flow file. Historically, this processor handled the merging by type and size or time prior to
creating a SequenceFile output; it no longer does this. If creating a SequenceFile that contains multiple files of the
same type is desired, precede this processor with a `RouteOnAttribute` processor to segregate files of the same type and
follow that with a `MergeContent` processor to bundle up files. If the type of files is not important, just use the
`MergeContent` processor. When using the `MergeContent` processor, the following Merge Formats are supported by this
processor:

* TAR
* ZIP
* FlowFileStream v3

The created SequenceFile is named the same as the incoming FlowFile with the suffix '.sf'. For incoming FlowFiles that
are bundled, the keys in the SequenceFile are the individual file names, the values are the contents of each file.

NOTE: The value portion of a key/value pair is loaded into memory. While there is a max size limit of 2GB, this could
cause memory issues if there are too many concurrent tasks and the flow file sizes are large.

## Using Compression

The value of the `Compression codec` property determines the compression library the processor uses to compress content.
Third party libraries are used for compression. These third party libraries can be Java libraries or native libraries.
In case of native libraries, the path of the parent folder needs to be in an environment variable called
`LD_LIBRARY_PATH` so that NiFi can find the libraries.

### Example: using Snappy compression with native library on CentOS

1. Snappy compression needs to be installed on the server running NiFi:  
   `sudo yum install snappy`

2. Suppose that the server running NiFi has the native compression libraries in `/opt/lib/hadoop/lib/native` . (Native
   libraries have file extensions like `.so`, `.dll`, `.lib`, etc. depending on the platform.)  
   We need to make sure that the files can be executed by the NiFi process' user. For this purpose we can make a copy of
   these files to e.g. `/opt/nativelibs` and change their owner. If NiFi is executed by `nifi` user in the `nifi` group,
   then:  
   `chown nifi:nifi /opt/nativelibs`  
   `chown nifi:nifi /opt/nativelibs/*`

3. The `LD_LIBRARY_PATH` needs to be set to contain the path to the folder `/opt/nativelibs`.

4. NiFi needs to be restarted.
5. `Compression codec` property can be set to `SNAPPY` and a `Compression type` can be selected.
6. The processor can be started.