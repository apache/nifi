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

# XMLReader

The XMLReader Controller Service reads XML content and creates Record objects. The Controller Service must be configured
with a schema that describes the structure of the XML data. Fields in the XML data that are not defined in the schema
will be skipped. Depending on whether the property "Expect Records as Array" is set to "false" or "true", the reader
either expects a single record or an array of records for each FlowFile.

Example: Single record

```xml
<record>
    <field1>content</field1>
    <field2>content</field2>
</record>
```

An array of records has to be enclosed by a root tag. Example: Array of records

```xml
<root>
    <record>
        <field1>content</field1>
        <field2>content</field2>
    </record>
    <record>
        <field1>content</field1>
        <field2>content</field2>
    </record>
</root>
```

## Example: Simple Fields

The simplest kind of data within XML data are tags / fields only containing content (no attributes, no embedded tags).
They can be described in the schema by simple types (e. g. INT, STRING, ...).

```xml
<root>
    <record>
        <simple_field>content</simple_field>
    </record>
</root>
```

This record can be described by a schema containing one field (e.g. of type string). By providing this schema, the
reader expects zero or one occurrences of "simple\_field" in the record.

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "simple_field",
      "type": "string"
    }
  ]
}
```

## Example: Arrays with Simple Fields

Arrays are considered as repetitive tags / fields in XML data. For the following XML data, "array\_field" is considered
to be an array enclosing simple fields, whereas "simple\_field" is considered to be a simple field not enclosed in an
array.

```xml
<record>
    <array_field>content</array_field>
    <array_field>content</array_field>
    <simple_field>content</simple_field>
</record>
```

This record can be described by the following schema:

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "array_field",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "simple_field",
      "type": "string"
    }
  ]
}
```

If a field in a schema is embedded in an array, the reader expects zero, one or more occurrences of the field in a
record. The field "array\_field" principally also could be defined as a simple field, but then the second occurrence of
this field would replace the first in the record object. Moreover, the field "simple\_field" could also be defined as an
array. In this case, the reader would put it into the record object as an array with one element.

## Example: Tags with Attributes

XML fields frequently not only contain content, but also attributes. The following record contains a field with an
attribute "attr" and content:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field</field_with_attribute>
</record>
```

To parse the content of the field "field\_with\_attribute" together with the attribute "attr", two requirements have to
be fulfilled:

* In the schema, the field has to be defined as record.
* The property "Field Name for Content" has to be set.
* As an option, the property "Attribute Prefix" also can be set.

For the example above, the following property settings are assumed:

| Property Name          | Property Value           |
|------------------------|--------------------------|
| Field Name for Content | `field_name_for_content` |
| Attribute Prefix       | `prefix_`                |

The schema can be defined as follows:

```json
{
  "name": "test",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "field_with_attribute",
      "type": {
        "name": "RecordForTag",
        "type": "record",
        "fields": [
          {
            "name": "attr",
            "type": "string"
          },
          {
            "name": "field_name_for_content",
            "type": "string"
          }
        ]
      }
      ]
    }
```

Note that the field "field\_name\_for\_content" not only has to be defined in the property section, but also in the
schema, whereas the prefix for attributes is not part of the schema. It will be appended when an attribute named "attr"
is found at the respective position in the XML data and added to the record. The record object of the above example will
be structured as follows:

```
Record (
    Record "field_with_attribute" (
        RecordField "prefix_attr" = "attr_content",
        RecordField "field_name_for_content" = "content of field"
    )
```

Principally, the field "field\_with\_attribute" could also be defined as a simple field. In this case, the attributes
simply would be ignored. Vice versa, the simple field in example 1 above could also be defined as a record (assuming
that the property "Field Name for Content" is set.

It is possible that the schema is not provided explicitly, but schema inference is used. For details on XML attributes
and schema inference, see "Example: Tags with Attributes and Schema Inference" below.

## Example: Tags within tags

XML data is frequently nested. In this case, tags enclose other tags:

```xml
<record>
    <field_with_embedded_fields attr="attr_content">
        <embedded_field>embedded content</embedded_field>
        <another_embedded_field>another embedded content</another_embedded_field>
    </field_with_embedded_fields>
</record>
```

The enclosing fields always have to be defined as records, irrespective whether they include attributes to be parsed or
not. In this example, the tag "field\_with\_embedded\_fields" encloses the fields "embedded\_field" and "
another\_embedded\_field", which are both simple fields. The schema can be defined as follows:

```json
{
  "name": "test",
  "namespace": "nifi",
  "type": "record",
  "fields": [
    {
      "name": "field_with_embedded_fields",
      "type": {
        "name": "RecordForEmbedded",
        "type": "record",
        "fields": [
          {
            "name": "attr",
            "type": "string"
          },
          {
            "name": "embedded_field",
            "type": "string"
          },
          {
            "name": "another_embedded_field",
            "type": "string"
          }
        ]
      }
      ]
    }
```

Notice that this case does not require the property "Field Name for Content" to be set as this is only required for tags
containing attributes and content.

## Example: Tags with Attributes and Schema Inference

When the record's schema is not provided but inferred based on the data itself, providing a value for the "Field Name
for Content" property is especially important. (For detailed information on schema inference, see the "Schema Inference"
section below.) Let's focus on cases where an XML element (called `<field_with_attribute>` in the examples) has an XML
attribute and some content and no sub-elements. For the examples below, let's assume that a ConvertRecord processor is
used, and it uses an XMLReader controller service and an XMLRecordSetWriter controller service. The settings for
XMLReader are provided separately for each example. The settings for XMLRecordSetWriter are common for all the examples
below. This way an XML to XML conversion is executed and comparing the input data with the output highlights the schema
inference behavior. The same behavior can be observed if a different Writer controller service is used.
XMLRecordSetWriter was chosen for these examples so that the input and the output are easily comparable. The settings of
the common XMLRecordSetWriter are the following:

| Property Name          | Property Value          |
|------------------------|-------------------------|
| Schema Access Strategy | `Inherit Record Schema` |
| Suppress Null Values   | `Never Suppress`        |

### XML Attributes and Schema Inference Example 1

The simplest case is when XML attributes are ignored completely during schema inference. To achieve this, the "Parse XML
Attributes" property in XMLReader is set to "false".

XMLReader settings:

| Property Name           | Property Value |
|-------------------------|----------------|
| Schema Access Strategy  | `Infer Schema` |
| Parse XML Attributes    | `false`        |
| Expect Records as Array | `false`        |
| Field Name for Content  | not set        |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field</field_with_attribute>
</record>
```

Output:

```xml
<record>
    <field_with_attribute>content of field</field_with_attribute>
</record>
```

If "Parse XML Attributes" is "false", the XML attribute is not parsed. Its name does not appear in the inferred schema
and its value is ignored. The reader behaves as if the XML attribute was not there.

Important note: "Field Name for Content" was not set in this example. This could lead to data loss if "
field\_with\_attribute" had child elements, similarly to what is described in "XML Attributes and Schema Inference
Example 2" and "XML Attributes and Schema Inference Example 4". To avoid that, "Field Name for Content" needs to be
assigned a value that is different from any existing XML tags in the data, like in "XML Attributes and Schema Inference
Example 6".

### XML Attributes and Schema Inference Example 2

XMLReader settings:

| Property Name           | Property Value |
|-------------------------|----------------|
| Schema Access Strategy  | `Infer Schema` |
| Parse XML Attributes    | `true`         |
| Expect Records as Array | `false`        |
| Field Name for Content  | not set        |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field</field_with_attribute>
</record>
```

As mentioned above, the element called "field\_with\_attribute" has an attribute and some content but no sub-element.

Output:

```xml
<record>
    <field_with_attribute>
        <attr>attr_content</attr>
        <value></value>
    </field_with_attribute>
</record>
```

In the XMLReader's settings, no value is set for the "Field Name for Content" property. In such cases the schema
inference logic adds a field named "value" to the schema. However, since "Field Name for Content" is not set, the data
processing logic is instructed not to consider the original content of the parent XML tags (`<field_with_attribute>` the
content of which is "content of field" in the example). So a new field named "value" appears in the schema but no value
is assigned to it from the data, thus the field is empty. The XML attribute (named "attr") is processed, a field named "
attr" is added to the schema and the attribute's value ("attr\_content") is assigned to it. In a case like this, the
parent field's original content is lost and a new field named "value" appears in the schema with no data assigned to it.
This is to make sure that no data is overwritten in the record if it already contains a field named "value". More on
that case in Example 4 and Example 5.

### XML Attributes and Schema Inference Example 3

In this example, the XMLReader's "Field Name for Content" property is filled with the value "original\_content". The
input data is the same as in the previous example.

XMLReader settings:

| Property Name           | Property Value     |
|-------------------------|--------------------|
| Schema Access Strategy  | `Infer Schema`     |
| Parse XML Attributes    | `true`             |
| Expect Records as Array | `false`            |
| Field Name for Content  | `original_content` |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field</field_with_attribute>
</record>
```

Output:

```xml
<record>
    <field_with_attribute>
        <attr>attr_content</attr>
        <original_content>content of field</original_content>
    </field_with_attribute>
</record>
```

The XMLReader's "Field Name for Content" property contains the value "original\_content" (the concrete value is not
important, what is important is that a value is provided and it does not clash with the name of any sub-element in
`<field_with_attribute>`). This explicitly tells the XMLReader controller service to create a field named "
original\_content" and make the original content of the parent XML tag the value of the field named "original\_content".
Adding the XML attributed named "attr" works just like in the first example. Since the `<field_with_attribute>` element
had no child-element with the name "original\_content", no data is lost.

### XML Attributes and Schema Inference Example 4

In this example, XMLReader's "Field Name for Content" property is left empty. In the input data, the
`<field_with_attribute>` element has some content and a sub-element named `<value>`.

XMLReader settings:

| Property Name           | Property Value |
|-------------------------|----------------|
| Schema Access Strategy  | `Infer Schema` |
| Parse XML Attributes    | `true`         |
| Expect Records as Array | `false`        |
| Field Name for Content  | not set        |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field<value>123</value>
    </field_with_attribute>
</record>
```

Output:

```xml
<record>
    <field_with_attribute>
        <attr>attr_content</attr>
        <value>123</value>
    </field_with_attribute>
</record>
```

The "Field Name for Content" property is not set, and the XML element has a sub-element named "value". The name of the
sub-element clashes with the default field name added to the schema by the Schema Inference logic (see Example 2). As
seen in the output data, the input XML attribute's value is added to the record just like in the previous examples. The
value of the `<value>` element is retained, but the content of the `<field_with_attribute>` that was outside the
sub-element, is lost.

### XML Attributes and Schema Inference Example 5

In this example, XMLReader's "Field Name for Content" property is given the value "value". In the input data, the
`<field_with_attribute>` element has some content and a sub-element named `<value>`. The name of the sub-element clashes
with the value of the "Field Name for Content" property.

XMLReader settings:

| Property Name           | Property Value |
|-------------------------|----------------|
| Schema Access Strategy  | `Infer Schema` |
| Parse XML Attributes    | `true`         |
| Expect Records as Array | `false`        |
| Field Name for Content  | `value`        |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field<value>123</value>
    </field_with_attribute>
</record>
```

Output:

```xml
<record>
    <field_with_attribute>
        <attr>attr_content</attr>
        <value>content of field</value>
    </field_with_attribute>
</record>
```

The "Field Name for Content" property's value is "value", and the XML element has a sub-element named "value". The name
of the sub-element clashes with the value of the "Field Name for Content" property. The value of the `<value>` element
is replaced by the content of the `<field_with_attribute>` element, and the original content of the `<value>` element is
lost.

### XML Attributes and Schema Inference Example 6

To avoid losing any data, the XMLReader's "Field Name for Content" property needs to be given a value that does not
clash with any sub-element's name in the input data. In this example the input data is the same as in the previous one,
but the "Field Name for Content" property's value is "original\_content", a value that does not clash with any
sub-element name. No data is lost in this case.

XMLReader settings:

| Property Name           | Property Value     |
|-------------------------|--------------------|
| Schema Access Strategy  | `Infer Schema`     |
| Parse XML Attributes    | `true`             |
| Expect Records as Array | `false`            |
| Field Name for Content  | `original_content` |

Input:

```xml
<record>
    <field_with_attribute attr="attr_content">content of field<value>123</value>
    </field_with_attribute>
</record>
```

Output:

```xml
<record>
    <field_with_attribute>
        <attr>attr_content</attr>
        <value>123</value>
        <original_content>content of field</original_content>
    </field_with_attribute>
</record>
```

It can be seen in the output data, that the attribute has been added to the `<field_with_attribute>` element as a
sub-element, the `<value>` retained its value, and the original content of the `<field_with_attribute>` element has been
added as a sub-element named "original\_content". This is because a value was chosen for the "Field Name for Content"
property that does not clash with any of the existing sub-elements of the input XML element (`<field_with_attribute>`).
No data is lost.

## Example: Array of records

For further explanation of the logic of this reader, an example of an array of records shall be demonstrated. The
following record contains the field "array\_field", which repeatedly occurs. The field contains two embedded fields.

```xml
<record>
    <array_field>
        <embedded_field>embedded content 1</embedded_field>
        <another_embedded_field>another embedded content 1</another_embedded_field>
    </array_field>
    <array_field>
        <embedded_field>embedded content 2</embedded_field>
        <another_embedded_field>another embedded content 2</another_embedded_field>
    </array_field>
</record>
```

This XML data can be parsed similarly to the data in example 4. However, the record defined in the schema of example 4
has to be embedded in an array.

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "array_field",
      "type": {
        "type": "array",
        "items": {
          "name": "RecordInArray",
          "type": "record",
          "fields": [
            {
              "name": "embedded_field",
              "type": "string"
            },
            {
              "name": "another_embedded_field",
              "type": "string"
            }
          ]
        }
      }
    }
  ]
}
```

## Example: Array in record

In XML data, arrays are frequently enclosed by tags:

```xml
<record>
    <field_enclosing_array>
        <element>content 1</element>
        <element>content 2</element>
    </field_enclosing_array>
    <field_without_array>content 3</field_without_array>
</record>
```

For the schema, embedded tags have to be described by records. Therefore, the field "field\_enclosing\_array" is a
record that embeds an array with elements of type string:

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "field_enclosing_array",
      "type": {
        "name": "EmbeddedRecord",
        "type": "record",
        "fields": [
          {
            "name": "element",
            "type": {
              "type": "array",
              "items": "string"
            }
          }
        ]
      }
    },
    {
      "name": "field_without_array",
      "type": "string"
    }
  ]
}
```

## Example: Maps

A map is a field embedding fields with different names:

```xml
<record>
    <map_field>
        <field1>content</field1>
        <field2>content</field2>                     ...
    </map_field>
    <simple_field>content</simple_field>
</record>
```

This data can be processed using the following schema:

```json
{
  "namespace": "nifi",
  "name": "test",
  "type": "record",
  "fields": [
    {
      "name": "map_field",
      "type": {
        "type": "map",
        "items": string
      }
    },
    {
      "name": "simple_field",
      "type": "string"
    }
  ]
}
```

## Schema Inference

While NiFi's Record API does require that each Record have a schema, it is often convenient to infer the schema based on
the values in the data, rather than having to manually create a schema. This is accomplished by selecting a value of "
Infer Schema" for the "Schema Access Strategy" property. When using this strategy, the Reader will determine the schema
by first parsing all data in the FlowFile, keeping track of all fields that it has encountered and the type of each
field. Once all data has been parsed, a schema is formed that encompasses all fields that have been encountered.

A common concern when inferring schemas is how to handle the condition of two values that have different types. For
example, consider a FlowFile with the following two records:

```xml
<root>
    <record>
        <name>John</name>
        <age>8</age>
        <values>N/A</values>
    </record>
    <record>
        <name>Jane</name>
        <age>Ten</age>
        <values>8</values>
        <values>Ten</values>
    </record>
</root>
```

It is clear that the "name" field will be inferred as a STRING type. However, how should we handle the "age" field?
Should the field be an CHOICE between INT and STRING? Should we prefer LONG over INT? Should we just use a STRING?
Should the field be considered nullable?

To help understand how this Record Reader infers schemas, we have the following list of rules that are followed in the
inference logic:

* All fields are inferred to be nullable.
* When two values are encountered for the same field in two different records (or two values are encountered for an
  ARRAY type), the inference engine prefers to use a "wider" data type over using a CHOICE data type. A data type "A" is
  said to be wider than data type "B" if and only if data type "A" encompasses all values of "B" in addition to other
  values. For example, the LONG type is wider than the INT type but not wider than the BOOLEAN type (and BOOLEAN is also
  not wider than LONG). INT is wider than SHORT. The STRING type is considered wider than all other types except MAP,
  RECORD, ARRAY, and CHOICE.
* If two values are encountered for the same field in two different records (or two values are encountered for an ARRAY
  type), but neither value is of a type that is wider than the other, then a CHOICE type is used. In the example above,
  the "values" field will be inferred as a CHOICE between a STRING or an ARRRAY<STRING>.
* If the "Time Format," "Timestamp Format," or "Date Format" properties are configured, any value that would otherwise
  be considered a STRING type is first checked against the configured formats to see if it matches any of them. If the
  value matches the Timestamp Format, the value is considered a Timestamp field. If it matches the Date Format, it is
  considered a Date field. If it matches the Time Format, it is considered a Time field. In the unlikely event that the
  value matches more than one of the configured formats, they will be matched in the order: Timestamp, Date, Time. I.e.,
  if a value matched both the Timestamp Format and the Date Format, the type that is inferred will be Timestamp. Because
  parsing dates and times can be expensive, it is advisable not to configure these formats if dates, times, and
  timestamps are not expected, or if processing the data as a STRING is acceptable. For use cases when this is
  important, though, the inference engine is intelligent enough to optimize the parsing by first checking several very
  cheap conditions. For example, the string's length is examined to see if it is too long or too short to match the
  pattern. This results in far more efficient processing than would result if attempting to parse each string value as a
  timestamp.
* The MAP type is never inferred. Instead, the RECORD type is used.
* If two elements exist with the same name and the same parent (i.e., two sibling elements have the same name), the
  field will be inferred to be of type ARRAY.
* If a field exists but all values are null, then the field is inferred to be of type STRING.

## Caching of Inferred Schemas

This Record Reader requires that if a schema is to be inferred, that all records be read in order to ensure that the
schema that gets inferred is applicable for all records in the FlowFile. However, this can become expensive, especially
if the data undergoes many different transformations. To alleviate the cost of inferring schemas, the Record Reader can
be configured with a "Schema Inference Cache" by populating the property with that name. This is a Controller Service
that can be shared by Record Readers and Record Writers.

Whenever a Record Writer is used to write data, if it is configured with a "Schema Cache," it will also add the schema
to the Schema Cache. This will result in an identifier for that schema being added as an attribute to the FlowFile.

Whenever a Record Reader is used to read data, if it is configured with a "Schema Inference Cache", it will first look
for a "schema.cache.identifier" attribute on the FlowFile. If the attribute exists, it will use the value of that
attribute to lookup the schema in the schema cache. If it is able to find a schema in the cache with that identifier,
then it will use that schema instead of reading, parsing, and analyzing the data to infer the schema. If the attribute
is not available on the FlowFile, or if the attribute is available but the cache does not have a schema with that
identifier, then the Record Reader will proceed to infer the schema as described above.

The end result is that users are able to chain together many different Processors to operate on Record-oriented data.
Typically, only the first such Processor in the chain will incur the "penalty" of inferring the schema. For all other
Processors in the chain, the Record Reader is able to simply lookup the schema in the Schema Cache by identifier. This
allows the Record Reader to infer a schema accurately, since it is inferred based on all data in the FlowFile, and still
allows this to happen efficiently since the schema will typically only be inferred once, regardless of how many
Processors handle the data.