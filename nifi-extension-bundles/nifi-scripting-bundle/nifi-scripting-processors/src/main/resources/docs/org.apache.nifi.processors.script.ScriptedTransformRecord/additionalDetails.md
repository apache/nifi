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

# ScriptedTransformRecord

### Description

The ScriptedTransformRecord provides the ability to use a scripting language, such as Groovy, to quickly and easily
update the contents of a Record. NiFi provides several different Processors that can be used to manipulate Records in
different ways. Each of these processors has its pros and cons. The ScriptedTransformRecord is perhaps the most powerful
and most versatile option. However, it is also the most error-prone, as it depends on writing custom scripts. It is also
likely to yield the lowest performance, as processors and libraries written directly in Java are likely to perform
better than interpreted scripts.

When creating a script, it is important to note that, unlike ExecuteScript, this Processor does not allow the script
itself to expose Properties to be configured or define Relationships. This is a deliberate decision. If it is necessary
to expose such configuration, the ExecuteScript processor should be used instead. By not exposing these elements, the
script avoids the need to define a Class or implement methods with a specific method signature. Instead, the script can
avoid any boilerplate code and focus purely on the task at hand.

The provided script is evaluated once for each Record that is encountered in the incoming FlowFile. Each time that the
script is invoked, it is expected to return a Record object (See note below regarding [Return Values](#ReturnValue)).
That Record is then written using the configured Record Writer. If the script returns a `null` value, the Record will
not be written. If the script returns an object that is not a Record, the incoming FlowFile will be routed to the
`failure` relationship.

This processor maintains two Counters: "Records Transformed" indicating the number of Records that were passed to the
script and for which the script returned a Record, and "Records Dropped" indicating the number of Records that were
passed to the script and for which the script returned a value of `null`.

### Variable Bindings

While the script provided to this Processor does not need to provide boilerplate code or implement any
classes/interfaces, it does need some way to access the Records and other information that it needs in order to perform
its task. This is accomplished by using Variable Bindings. Each time that the script is invoked, each of the following
variables will be made available to the script:

| Variable Name | Description                                                                                                                                                                                                                                                                                                  | Variable Class                                                                                                          |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| record        | The Record that is to be transformed.                                                                                                                                                                                                                                                                        | [Record](https://javadoc.io/static/org.apache.nifi/nifi-record/1.11.4/org/apache/nifi/serialization/record/Record.html) |
| recordIndex   | The zero-based index of the Record in the FlowFile.                                                                                                                                                                                                                                                          | Long (64-bit signed integer)                                                                                            |
| log           | The Processor's Logger. Anything that is logged to this logger will be written to the logs as if the Processor itself had logged it. Additionally, a bulletin will be created for any log message written to this logger (though by default, the Processor will hide any bulletins with a level below WARN). | [ComponentLog](https://www.javadoc.io/doc/org.apache.nifi/nifi-api/latest/org/apache/nifi/logging/ComponentLog.html)    |
| attributes    | Map of key/value pairs that are the Attributes of the FlowFile. Both the keys and the values of this Map are of type String. This Map is immutable. Any attempt to modify it will result in an UnsupportedOperationException being thrown.                                                                   | java.util.Map                                                                                                           |

### Return Value

Each time that the script is invoked, it is expected to return
a [Record](https://javadoc.io/static/org.apache.nifi/nifi-record/1.11.4/org/apache/nifi/serialization/record/Record.html)
object or a Collection of Record objects. Those Records are then written using the configured Record Writer. If the
script returns a `null` value, the Record will not be written. If the script returns an object that is not a Record or
Collection of Records, the incoming FlowFile will be routed to the `failure` relationship.

The Record that is provided to the script is mutable. Therefore, it is a common pattern to update the `record` object in
the script and simply return that same `record` object.

**Note:** Depending on the scripting language, a script with no explicit return value may return `null` or may return
the last value that was referenced. Because returning `null` will result in dropping the Record and a non-Record return
value will result in an Exception (and simply for the sake of clarity), it is important to ensure that the configured
script has an explicit return value.

### Adding a New Fields

A very common usage of Record-oriented processors is to allow the Record Reader to infer its schema and have the Record
Writer inherit the Record's schema. In this scenario, it is important to note that the Record Writer will inherit the
schema of the first Record that it encounters. Therefore, if the configured script will add a new field to a Record, it
is important to ensure that the field is added to all Records (with a `null` value where appropriate).

See the [Adding New Fields](#AddingFieldExample) example for more details.

### Performance Considerations

NiFi offers many different processors for updating records in various ways. While each of these has its own pros and
cons, performance is often an important consideration. It is generally the case that standard processors, such as
UpdateRecord, will perform better than script-oriented processors. However, this may not always be the case. For
situations when performance is critical, the best case is to test both approaches to see which performs best.

A simple 5-minute benchmark was done to analyze the difference in performance. The script used simply modifies one field
and return the Record otherwise unmodified. The results are shown below. Note that no specifics are given regarding
hardware, specifically because the results should not be used to garner expectations of absolute performance but rather
to show relative performance between the different options.

| Processor                                        | Script Used                                                          | Records processed in 5 minutes |
|--------------------------------------------------|----------------------------------------------------------------------|--------------------------------|
| UpdateAttribute                                  | _No Script. User-defined Property added with name /num and value 42_ | 50.1 million                   |
| ScriptedTransformRecord - Using Language: Groovy | **record.setValue("num", 42)**<br>**record**                         | 18.9 million                   |

## Example Scripts

### Remove First Record

The following script will remove the first Record from each FlowFile that it encounters.

Example Input (CSV):

```
name, num Mark, 42 Felicia, 3720 Monica, -3
```

Example Output (CSV):

```
name, num Felicia, 3720 Monica, -3
```

Example Script (Groovy):

```groovy
return recordIndex == 0 ? null : record
```

### Replace Field Value

The following script will replace any field in a Record if the value of that field is equal to the value of the "Value
To Replace" attribute. The value of that field will be replaced with whatever value is in the "Replacement Value"
attribute.

Example Input Content (JSON):

```json
[
  {
    "book": {
      "author": "John Doe",
      "date": "01/01/1980"
    }
  },
  {
    "book": {
      "author": "Jane Doe",
      "date": "01/01/1990"
    }
  }
]
```

Example Input Attributes:

| Attribute Name    | Attribute Value |
|-------------------|-----------------|
| Value To Replace  | Jane Doe        |
| Replacement Value | Author Unknown  |

Example Output (JSON):

```json
[
  {
    "book": {
      "author": "John Doe",
      "date": "01/01/1980"
    }
  },
  {
    "book": {
      "author": "Author Unknown",
      "date": "01/01/1990"
    }
  }
]
```

Example Script (Groovy):

```groovy
def replace(rec) {
    rec.toMap().each { k, v ->
        // If the field value is equal to the attribute 'Value to Replace', then set the         
        // field value to the 'Replacement Value' attribute.         
        if (v?.toString()?.equals(attributes['Value to Replace'])) {
            rec.setValue(k, attributes['Replacement Value'])
        }

        // Call Recursively if the value is a Record         
        if (v instanceof org.apache.nifi.serialization.record.Record) {
            replace(v)
        }
    }
}

replace(record)
return record
```

### Pass-through

The following script allows each Record to pass through without altering the Record in any way.

Example Input: <any>

Example output: <identical to input>

Example Script (Groovy):

```groovy
record
```

### Adding New Fields

The following script adds a new field named "favoriteColor" to all Records. Additionally, it adds an "isOdd" field to
all even-numbered Records.

It is important that all Records have the same schema. Since we want to add an "isOdd" field to Records 1 and 3, the
schema for Records 0 and 2 must also account for this. As a result, we will add the field to all Records but use a null
value for Records that are not even. See [Adding New Fields](#AddingNewFields) for more information.

Example Input Content (CSV):

```
name, favoriteFood John Doe, Spaghetti Jane Doe, Pizza Jake Doe, Sushi June Doe, Hamburger
```

Example Output (CSV):

```
name, favoriteFood, favoriteColor, isOdd John Doe, Spaghetti, Blue, Jane Doe, Pizza, Blue, true Jake Doe, Sushi, Blue, June Doe, Hamburger, Blue, true
```

Example Script (Groovy):

```groovy
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

// Always set favoriteColor to Blue. // Because we are calling #setValue with a String as the field name, the field type will be inferred. record.setValue("favoriteColor", "Blue")  // Set the 'isOdd' field to true if the record index is odd. Otherwise, set the 'isOdd' field to `null`. // Because the value may be `null` for the first Record (in fact, it always will be for this particular case), // we need to ensure that the Record Writer's schema be given the correct type for the field. As a result, we will not call // #setValue with a String as the field name but rather will pass a RecordField as the first argument, as the RecordField // allows us to specify the type of the field. // Also note that `RecordField` and `RecordFieldType` are `import`ed above. record.setValue(new RecordField("isOdd", RecordFieldType.BOOLEAN.getDataType()), recordIndex % 2 == 1 ? true : null)  return record
```

### Fork Record

The following script return each Record that it encounters, plus another Record, which is derived from the first, but
where the 'num' field is one less than the 'num' field of the input.

Example Input (CSV):

```
name, num Mark, 42 Felicia, 3720 Monica, -3
```

Example Output (CSV):

```
name, num Mark, 42 Mark, 41 Felicia, 3720 Felicia, 3719 Monica, -3 Monica, -4
```

Example Script (Groovy):

```groovy
import org.apache.nifi.serialization.record.*

def derivedValues = new HashMap(record.toMap())
derivedValues.put('num', derivedValues['num'] - 1)
derived = new MapRecord(record.schema, derivedValues)
return [record, derived]
```