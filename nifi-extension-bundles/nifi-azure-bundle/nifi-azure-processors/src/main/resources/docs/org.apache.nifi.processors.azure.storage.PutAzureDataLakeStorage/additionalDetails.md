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

# PutAzureDataLakeStorage

This processor is responsible for uploading files to Azure Data Lake Storage Gen2.

### File uploading and cleanup process in case of "Write and Rename" strategy

#### New file upload

1. A temporary file is created with random prefix under the given path in '\_nifitempdirectory'.
2. Content is appended to temp file.
3. Temp file is moved to the final destination directory and renamed to its original name.
4. In case of appending or renaming failure, the temp file is deleted.
5. In case of temporary file deletion failure, the temp file remains on the server.

#### Existing file upload

* Processors with "fail" conflict resolution strategy will direct the FlowFile to "Failure" relationship.
* Processors with "ignore" conflict resolution strategy will direct the FlowFile to "Success" relationship.
* Processors with "replace" conflict resolution strategy:

1. A temporary file is created with random prefix under the given path in '\_nifitempdirectory'.
2. Content is appended to temp file.
3. Temp file is moved to the final destination directory and renamed to its original name, the original file is
   overwritten.
4. In case of appending or renaming failure, the temp file is deleted and the original file remains intact.
5. In case of temporary file deletion failure, both temp file and original file remain on the server.

### File uploading and cleanup process in case of "Simple Write" strategy

#### New file upload

1. An empty file is created at its final destination.
2. Content is appended to the file.
3. In case of appending failure, the file is deleted.
4. In case of file deletion failure, the file remains on the server.

#### Existing file upload

* Processors with "fail" conflict resolution strategy will direct the FlowFile to "Failure" relationship.
* Processors with "ignore" conflict resolution strategy will direct the FlowFile to "Success" relationship.
* Processors with "replace" conflict resolution strategy:

1. An empty file is created at its final destination, the original file is overwritten.
2. Content is appended to the file.
3. In case of appending failure, the file is deleted and the original file is not restored.
4. In case of file deletion failure, the file remains on the server.