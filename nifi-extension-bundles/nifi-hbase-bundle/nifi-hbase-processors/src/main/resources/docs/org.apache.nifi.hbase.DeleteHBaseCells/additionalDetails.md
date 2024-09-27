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

# DeleteHBaseCells

## Overview

This processor provides the ability to do deletes against one or more HBase cells, without having to delete the entire
row. It should be used as the primary delete method when visibility labels are in use and the cells have different
visibility labels. Each line in the flowfile body is a fully qualified cell (row id, column family, column qualifier and
visibility labels if applicable). The separator that separates each piece of the fully qualified cell is configurable,
but **::::** is the default value.

## Example FlowFile

```
row1::::user::::name
row1::::user::::address::::PII
row1::::user::::billing\_code\_1::::PII&&BILLING
```
