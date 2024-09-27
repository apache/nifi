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

# PutHBaseJSON

## Visibility Labels

This processor provides the ability to attach visibility labels to HBase Puts that it generates, if visibility labels
are enabled on the HBase cluster. There are two ways to enable this:

* Attributes on the flowfile.
* Dynamic properties added to the processor.

When the dynamic properties are defined on the processor, they will be the default value, but can be overridden by
attributes set on the flowfile. The naming convention for both (property name and attribute name) is:

* visibility.COLUMN\_FAMILY - every column qualifier under the column family will get this.
* visibility.COLUMN\_FAMILY.COLUMN\_VISIBILITY - the qualified column qualifier will be assigned this value.