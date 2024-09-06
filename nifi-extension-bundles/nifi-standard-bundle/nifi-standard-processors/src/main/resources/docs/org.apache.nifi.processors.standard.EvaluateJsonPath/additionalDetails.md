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

# EvaluateJsonPath

**Note:** The underlying JsonPath library loads the entirety of the streamed content into and performs result
evaluations in memory. Accordingly, it is important to consider the anticipated profile of content being evaluated by
this processor and the hardware supporting it especially when working against large JSON documents.

### Additional Notes

It's a common pattern to make JSON from attributes in NiFi. Many of these attributes have periods in their names. For
example _record.count_. To reference them safely, you must use this sort of operation which puts the entire key in
brackets. This also applies to JSON keys that contain whitespace:

> $.\["record.count"\]

> $.\["record count"\]