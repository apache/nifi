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

# IdentifyMimeType

The following is a non-exhaustive list of MIME Types detected by default in NiFi:

* application/gzip
* application/bzip2
* application/flowfile-v3
* application/flowfile-v1
* application/xml
* video/mp4
* video/x-m4v
* video/mp4a-latm
* video/quicktime
* video/mpeg
* audio/wav
* audio/mp3
* image/bmp
* image/png
* image/jpg
* image/gif
* image/tif
* application/vnd.ms-works
* application/msexcel
* application/mspowerpoint
* application/msaccess
* application/x-ms-wmv
* application/pdf
* application/x-rpm
* application/tar
* application/x-7z-compressed
* application/java-archive
* application/zip
* application/x-lzh

An example value for the Config Body property that will identify a file whose contents start with "abcd" as MIME Type "
custom/abcd" and with extension ".abcd" would look like the following:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mime-info>
    <mime-type type="custom/abcd">
        <magic priority="50">
            <match value="abcd" type="string" offset="0"/>
        </magic>
        <glob pattern="\*.abcd"/>
    </mime-type>
</mime-info>
```

For a more complete list of Tika's default types (and additional details regarding customization of the value for the
Config Body property), please refer
to [Apache Tika's documentation](https://tika.apache.org/1.22/detection.html#Mime_Magic_Detection)