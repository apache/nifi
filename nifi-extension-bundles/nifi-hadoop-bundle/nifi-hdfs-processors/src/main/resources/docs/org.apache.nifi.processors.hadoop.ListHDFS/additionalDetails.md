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

# ListHDFS

## ListHDFS Filter Modes

There are three filter modes available for ListHDFS that determine how the regular expression in the `File Filter`
property will be applied to listings in HDFS.

* **Directories and Files**
  Filtering will be applied to the names of directories and files. If `Recurse Subdirectories` is set to true, only
  subdirectories with a matching name will be searched for files that match the regular expression defined in
  `File Filter`.
* **Files Only**
  Filtering will only be applied to the names of files. If `Recurse Subdirectories` is set to true, the entire
  subdirectory tree will be searched for files that match the regular expression defined in `File Filter`.
* **Full Path**
  Filtering will be applied to the full path of files. If `Recurse Subdirectories` is set to true, the entire
  subdirectory tree will be searched for files in which the full path of the file matches the regular expression defined
  in `File Filter`. Regarding `scheme` and `authority`, if a given file has a full path of
  `hdfs://hdfscluster:8020/data/txt/1.txt`, the filter will evaluate the regular expression defined in `File Filter`
  against two cases, matching if either is true:


* the full path including the scheme (`hdfs`), authority (`hdfscluster:8020`), and the remaining path components (
  `/data/txt/1.txt`)
* only the path components (`/data/txt/1.txt`)

## Examples:

For the given examples, the following directory structure is used:

data  
├── readme.txt  
├── bin  
│ ├── readme.txt  
│ ├── 1.bin  
│ ├── 2.bin  
│ └── 3.bin  
├── csv  
│ ├── readme.txt  
│ ├── 1.csv  
│ ├── 2.csv  
│ └── 3.csv  
└── txt ├── readme.txt ├── 1.txt ├── 2.txt └── 3.txt

### **Directories and Files**

This mode is useful when the listing should match the names of directories and files with the regular expression defined
in `File Filter`. When `Recurse Subdirectories` is true, this mode allows the user to filter for files in
subdirectories with names that match the regular expression defined in `File Filter`.

ListHDFS configuration:

| **Property**             | **Value**               |
|--------------------------|-------------------------|
| `Directory`              | `/data`                 |
| `Recurse Subdirectories` | true                    |
| `File Filter`            | `.*txt.*`               |
| `Filter Mode`            | `Directories and Files` |

ListHDFS results:

* /data/readme.txt
* /data/txt/readme.txt
* /data/txt/1.txt
* /data/txt/2.txt
* /data/txt/3.txt

### **Files Only**

This mode is useful when the listing should match only the names of files with the regular expression defined in
`File Filter`. Directory names will not be matched against the regular expression defined in `File Filter`. When
`Recurse Subdirectories` is true, this mode allows the user to filter for files in the entire subdirectory tree of
the directory specified in the `Directory` property.

ListHDFS configuration:

| **Property**             | **Value**      |
|--------------------------|----------------|
| `Directory`              | `/data`        |
| `Recurse Subdirectories` | true           |
| `File Filter`            | `[^\.].*\.txt` |
| `Filter Mode`            | `Files Only`   |

ListHDFS results:

* /data/readme.txt
* /data/bin/readme.txt
* /data/csv/readme.txt
* /data/txt/readme.txt
* /data/txt/1.txt
* /data/txt/2.txt
* /data/txt/3.txt

### **Full Path**

This mode is useful when the listing should match the entire path of a file with the regular expression defined in
`File Filter`. When `Recurse Subdirectories` is true, this mode allows the user to filter for files in the entire
subdirectory tree of the directory specified in the `Directory` property while allowing filtering based on the full
path of each file.

ListHDFS configuration:

| **Property**             | **Value**       |
|--------------------------|-----------------|
| `Directory`              | `/data`         |
| `Recurse Subdirectories` | true            |
| `File Filter`            | `(/.*/)*csv/.*` |
| `Filter Mode`            | `Full Path`     |

ListHDFS results:

* /data/csv/readme.txt
* /data/csv/1.csv
* /data/csv/2.csv
* /data/csv/3.csv

## Streaming Versus Batch Processing

ListHDFS performs a listing of all files that it encounters in the configured HDFS directory. There are two common,
broadly defined use cases.

### Streaming Use Case

By default, the Processor will create a separate FlowFile for each file in the directory and add attributes for
filename, path, etc. A common use case is to connect ListHDFS to the FetchHDFS processor. These two processors used in
conjunction with one another provide the ability to easily monitor a directory and fetch the contents of any new file as
it lands in HDFS in an efficient streaming fashion.

### Batch Use Case

Another common use case is the desire to process all newly arriving files in a given directory, and to then perform some
action only when all files have completed their processing. The above approach of streaming the data makes this
difficult, because NiFi is inherently a streaming platform in that there is no "job" that has a beginning and an end.
Data is simply picked up as it becomes available.

To solve this, the ListHDFS Processor can optionally be configured with a Record Writer. When a Record Writer is
configured, a single FlowFile will be created that will contain a Record for each file in the directory, instead of a
separate FlowFile per file. See the documentation for ListFile for an example of how to build a dataflow that allows for
processing all the files before proceeding with any other step.

One important difference between the data produced by ListFile and ListHDFS, though, is the structure of the Records
that are emitted. The Records emitted by ListFile have a different schema than those emitted by ListHDFS. ListHDFS emits
records that follow the following schema (in Avro format):

```json
{
  "type": "record",
  "name": "nifiRecord",
  "namespace": "org.apache.nifi",
  "fields": [
    {
      "name": "filename",
      "type": "string"
    },
    {
      "name": "path",
      "type": "string"
    },
    {
      "name": "directory",
      "type": "boolean"
    },
    {
      "name": "size",
      "type": "long"
    },
    {
      "name": "lastModified",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "permissions",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "owner",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "group",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "replication",
      "type": [
        "null",
        "int"
      ]
    },
    {
      "name": "symLink",
      "type": [
        "null",
        "boolean"
      ]
    },
    {
      "name": "encrypted",
      "type": [
        "null",
        "boolean"
      ]
    },
    {
      "name": "erasureCoded",
      "type": [
        "null",
        "boolean"
      ]
    }
  ]
}
```