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

# ScriptedPartitionRecord

### Description

The ScriptedPartitionRecord provides the ability to use a scripting language, such as Groovy, to quickly and easily
partition a Record based on its contents. There are multiple ways to reach the same behaviour such as using
PartitionRecord but working with user provided scripts opens a wide range of possibilities on the decision logic of
partitioning the individual records.

The provided script is evaluated once for each Record that is encountered in the incoming FlowFile. Each time that the
script is invoked, it is expected to return an `object` or a `null` value. The string representation of the return value
is used as the record's "partition". The `null` value is handled separately without conversion into string. All Records
with the same partition then will be batched to one FlowFile and routed to the `success` Relationship.

This Processor maintains a Counter with the name of "Records Processed". This represents the number of processed Records
regardless of partitioning.

### Variable Bindings

While the script provided to this Processor does not need to provide boilerplate code or implement any
classes/interfaces, it does need some way to access the Records and other information that it needs in order to perform
its task. This is accomplished by using Variable Bindings. Each time that the script is invoked, each of the following
variables will be made available to the script:

| Variable Name | Description                                                                                                                                                                                                                                                                                                  | Variable Class                                                                                                           |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| record        | The Record that is to be processed.                                                                                                                                                                                                                                                                          | [Record](https://www.javadoc.io/doc/org.apache.nifi/nifi-record/latest/org/apache/nifi/serialization/record/Record.html) |
| recordIndex   | The zero-based index of the Record in the FlowFile.                                                                                                                                                                                                                                                          | Long (64-bit signed integer)                                                                                             |
| log           | The Processor's Logger. Anything that is logged to this logger will be written to the logs as if the Processor itself had logged it. Additionally, a bulletin will be created for any log message written to this logger (though by default, the Processor will hide any bulletins with a level below WARN). | [ComponentLog](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/logging/ComponentLog.html)     |
| attributes    | Map of key/value pairs that are the Attributes of the FlowFile. Both the keys and the values of this Map are of type String. This Map is immutable. Any attempt to modify it will result in an UnsupportedOperationException being thrown.                                                                   | java.util.Map                                                                                                            |

### Return Value

The script is invoked separately for each Record. It is acceptable to return any Object might be represented as string.
This string value will be used as the partition of the given Record. Additionally, the script may return `null`.

## Example

The following script will partition the input on the value of the "stellarType" field.

Example Input (CSV):

```
starSystem, stellarType Wolf 359, M Epsilon Eridani, K Tau Ceti, G Groombridge 1618, K Gliese 1, M
```

Example Output 1 (CSV) - for partition "M":

```
starSystem, stellarType Wolf 359,M Gliese 1,M
```

Example Output 2 (CSV) - for partition "K":

```
starSystem, stellarType Epsilon Eridani,K Groombridge 1618,K
```

Example Output 3 (CSV) - for partition "G":

```
starSystem, stellarType Tau Ceti,G
```

Note: the order of the outgoing FlowFiles is not guaranteed.

Example Script (Groovy):

```groovy
return record.getValue("stellarType")
```