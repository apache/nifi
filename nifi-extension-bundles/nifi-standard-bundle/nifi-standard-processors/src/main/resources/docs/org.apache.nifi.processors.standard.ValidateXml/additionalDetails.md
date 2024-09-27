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

# ValidateCsv

## Usage Information

In order to fully validate XML, a schema must be provided. The ValidateXML processor allows the schema to be specified
in the property 'Schema File'. The following example illustrates how an XSD schema and XML data work together.

Example XSD specification

```xml
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://namespace/1"
           xmlns:tns="http://namespace/1" elementFormDefault="unqualified">
    <xs:element name="bundle" type="tns:BundleType"></xs:element>

    <xs:complexType name="BundleType">
        <xs:sequence>
            <xs:element name="node" type="tns:NodeType" maxOccurs="unbounded" minOccurs="0"></xs:element>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="NodeType">
        <xs:sequence>
            <xs:element name="subNode" type="tns:SubNodeType" maxOccurs="unbounded" minOccurs="0"></xs:element>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="SubNodeType">
        <xs:sequence>
            <xs:element name="value" type="xs:string"></xs:element>
        </xs:sequence>
    </xs:complexType>
</xs:schema>
```

Given the schema defined in the above XSD, the following are valid XML data.

```xml
<ns:bundle xmlns:ns="http://namespace/1">
    <node>
        <subNode>
            <value>Hello</value>
        </subNode>
        <subNode>
            <value>World!</value>
        </subNode>
    </node>
</ns:bundle>
```

```xml
<ns:bundle xmlns:ns="http://namespace/1">
    <node>
        <subNode>
            <value>Hello World!</value>
        </subNode>
    </node>
</ns:bundle>
```

The following are invalid XML data. The resulting validatexml.invalid.error attribute is shown.

```xml
<ns:bundle xmlns:ns="http://namespace/1">
    <node>Hello World!</node>
</ns:bundle>
```

```
validatexml.invalid.error: cvc-complex-type.2.3: Element 'node' cannot have character \[children\], because the type's content type is element-only.
```

```xml
<ns:bundle xmlns:ns="http://namespace/1">
    <node>
        <value>Hello World!</value>
    </node>
</ns:bundle>
```

```
validatexml.invalid.error: cvc-complex-type.2.4.a: Invalid content was found starting with element 'value'. One of '{subNode}' is expected.
```
