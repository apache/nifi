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

# EvaluateBaseXQuery

## Description:

This Processor evaluates an XQuery expression against the content of a Flow File using the BaseX XML database engine.
The Flow File content must be valid XML. The result of the query can be written back to the content or stored in a
Flow File attribute, depending on the processor configuration.

This processor is useful for filtering, transforming, or extracting values from XML documents using standard XQuery syntax.

**Processor's static properties:**

* **XQuery Expression** – the XQuery to evaluate against the Flow File content. You can reference FlowFile attributes using NiFi Expression Language (e.g., `${attribute}`).
* **Return Type** – determines where the result will be written. Possible values: `content`, `attribute`.
* **Output Attribute Name** (used only if Return Type = attribute) – name of the attribute to store the result in.

**Processor's dynamic properties:**

This processor does not use dynamic properties.

For more about XQuery syntax and BaseX, see:
[BaseX Documentation](https://docs.basex.org/wiki/XQuery)