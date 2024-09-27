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

# ScriptedFilterRecord

### Description

The ScriptedFilterRecord Processor provides the ability to use a scripting language, such as Groovy in order to remove
Records from an incoming FlowFile. NiFi provides several different Processors that can be used to work with Records in
different ways. Each of these processors has its pros and cons. The ScriptedFilterRecord is intended to work together
with these processors and be used as a pre-processing step before processing the FlowFile with more performance
consuming Processors, like ScriptedTransformRecord.

The Processor expects a user defined script in order to determine which Records should be kept and filtered out. When
creating a script, it is important to note that, unlike ExecuteScript, this Processor does not allow the script itself
to expose Properties to be configured or define Relationships.

The provided script is evaluated once for each Record that is encountered in the incoming FlowFile. Each time that the
script is invoked, it is expected to return a `boolean` value, which is used as a basis of filtering: For Records the
script returns with a `true` value, the given Record will be included to the outgoing FlowFile which will be routed to
the `success` Relationship. For `false` values the given Record will not be added to the output. In addition to this the
incoming FlowFile will be transferred to the `original` Relationship without change. If the script returns an object
that is not considered as `boolean`, the incoming FlowFile will be routed to the `failure` Relationship instead and no
FlowFile will be routed to the `success` Relationship.

This Processor maintains a Counter: "Records Processed" indicating the number of Records that were passed to the script
regardless of the result of the filtering.

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

Each time the script is invoked, it is expected to return a `boolean` value. Return values other than `boolean`,
including `null` value will be handled as unexpected script behaviour and handled accordingly: the processing will be
interrupted and the incoming FlowFile will be transferred to the `failure` relationship without further execution.

## Example Scripts

### Filtering based on position

The following script will keep only the first 2 Records from a FlowFile and filter out all the rest.

Example Input (CSV):

```
name, allyOf Decelea, Athens Corinth, Sparta Mycenae, Sparta Potidaea, Athens
```

Example Output (CSV):

```
name, allyOf Decelea, Athens Corinth, Sparta
```

Example Script (Groovy):

```groovy
return recordIndex < 2 ? true : false
```

### Filtering based on Record contents

The following script will filter the Records based on their content. Any Records satisfies the condition will be part of
the FlowFile routed to the `success` Relationship.

Example Input (JSON):

```json
[
  {
    "city": "Decelea",
    "allyOf": "Athens"
  },
  {
    "city": "Corinth",
    "allyOf": "Sparta"
  },
  {
    "city": "Mycenae",
    "allyOf": "Sparta"
  },
  {
    "city": "Potidaea",
    "allyOf": "Athens"
  }
]
```

Example Output (CSV):

```json
[
  {
    "city": "Decelea",
    "allyOf": "Athens"
  },
  {
    "city": "Potidaea",
    "allyOf": "Athens"
  }
]
```

Example Script (Groovy):

```groovy
if (record.getValue("allyOf") == "Athens") {
    return true;
} else {
    return false;
}
```