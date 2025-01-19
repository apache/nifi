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

# GenerateFlowFile

This processor can be configured to generate variable-sized FlowFiles. The `File Size` property accepts both a literal
value, e.g. "1 KB", and Expression Language statements. In order to create FlowFiles of variable sizes, the Expression
Language function `random()` can be used. For example, `${random():mod(101)}` will generate values between 0 and 100,
inclusive.  A data size label, e.g. B, KB, MB, etc., must be  included in the Expression Language statement since the
`File Size` property holds a data size value. The table below shows some examples.

| File Size Expression Language Statement    | File Sizes Generated (values are inclusive) |
|--------------------------------------------|---------------------------------------------|
| ${random():mod(101)}b                      | 0 - 100 bytes                               |
| ${random():mod(101)}mb                     | 0 - 100 MB                                  |
| ${random():mod(101):plus(20)} B            | 20 - 120 bytes                              |
| ${random():mod(71):plus(30):append("KB")}  | 30 - 100 KB                                 |

See the Expression Language Guide for more details on the `random()` function.