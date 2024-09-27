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

# Google Vision

## Google Cloud Vision - Get Annotate Images Status

### Usage

GetGcpVisionAnnotateImagesOperationStatus is designed to periodically check the statuses of image annotation operations.
This processor should be used in pair with StartGcpVisionAnnotateImagesOperation Processor. An outgoing FlowFile
contains the raw response returned by the Vision server. This response is in JSON format and contains a Google storage
reference where the result is located, as well as additional metadata, as written in
the [Google Vision API Reference document](https://cloud.google.com/vision/docs/reference/rest/v1/locations.operations#Operation).