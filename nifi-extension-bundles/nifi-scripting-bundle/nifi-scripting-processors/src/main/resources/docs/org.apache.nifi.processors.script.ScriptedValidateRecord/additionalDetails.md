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

# ScriptedValidateRecord

### Description

The ScriptedValidateRecord Processor provides the ability to use a scripting language, such as Groovy or Jyton in order
to validate Records in an incoming FlowFile. The provided script will be evaluated against each Record in an incoming
FlowFile. Each of those records will then be routed to either the "valid" or "invalid" FlowFile. As a result, each
incoming FlowFile may be broken up into two individual FlowFiles (if some records are valid and some are invalid,
according to the script), or the incoming FlowFile may have all of its Records kept together in a single FlowFile,
routed to either "valid" or "invalid" (if all records are valid or if all records are invalid, according to the script).

The Processor expects a user defined script in order to determine the validity of the Records. When creating a script,
it is important to note that, unlike ExecuteScript, this Processor does not allow the script itself to expose Properties
to be configured or define Relationships.

The provided script is evaluated once for each Record that is encountered in the incoming FlowFile. Each time that the
script is invoked, it is expected to return a `boolean` value, which is used to determine if the given Record is valid
or not: For Records the script returns with a `true` value, the given Record is considered valid and will be included to
the outgoing FlowFile which will be routed to the `valid` Relationship. For `false` values the given Record will be
added to the FlowFile routed to the `invalid` Relationship. Regardless of the number of incoming Records the outgoing
Records will be batched. For one incoming FlowFile there could be no more than one FlowFile routed to the `valid` and
the `invalid` Relationships. In case of there are no valid or invalid Record presents there will be no transferred
FlowFile for the respected Relationship. In addition to this the incoming FlowFile will be transferred to the `original`
Relationship without change. If the script returns an object that is not considered as `boolean`, the incoming FlowFile
will be routed to the `failure` Relationship instead and no FlowFile will be routed to the `valid` or `invalid`
Relationships.

This Processor maintains a Counter: "Records Processed" indicating the number of Records were processed by the
Processor.

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

### Validating based on position

The following script will consider only the first 2 Records as valid.

Example Input (CSV):

```
company, numberOfTrains Boston & Maine Railroad, 3 Chesapeake & Ohio Railroad, 2 Pennsylvania Railroad, 4 Reading Railroad, 2
```

Example Output (CSV) - valid Relationship:

```
company, numberOfTrains Boston & Maine Railroad, 3 Chesapeake & Ohio Railroad, 2
```

Example Output (CSV) - invalid Relationship:

```
company, numberOfTrains Pennsylvania Railroad, 4 Reading Railroad, 2
```

Example Script (Groovy):

```groovy
return recordIndex < 2 ? true : false
```

### Validating based on Record contents

The following script will filter the Records based on their content. Any Records satisfies the condition will be part of
the FlowFile routed to the `valid` Relationship, others wil lbe routed to the `invalid` Relationship.

Example Input (JSON):

```json
[
  {
    "company": "Boston & Maine Railroad",
    "numberOfTrains": 3
  },
  {
    "company": "Chesapeake & Ohio Railroad",
    "numberOfTrains": -1
  },
  {
    "company": "Pennsylvania Railroad",
    "numberOfTrains": 2
  },
  {
    "company": "Reading Railroad",
    "numberOfTrains": 4
  }
]
```

Example Output (CSV) - valid Relationship:

```json
[
  {
    "company": "Boston & Maine Railroad",
    "numberOfTrains": 3
  },
  {
    "company": "Pennsylvania Railroad",
    "numberOfTrains": 2
  },
  {
    "company": "Reading Railroad",
    "numberOfTrains": 4
  }
]
```

Example Output (CSV) - invalid Relationship:

```json
[
  {
    "company": "Chesapeake & Ohio Railroad",
    "numberOfTrains": -1
  }
]
```

Example Script (Groovy):

```groovy
if (record.getValue("numberOfTrains").toInteger() >= 0) {
    return true;
} else {
    return false;
}
```