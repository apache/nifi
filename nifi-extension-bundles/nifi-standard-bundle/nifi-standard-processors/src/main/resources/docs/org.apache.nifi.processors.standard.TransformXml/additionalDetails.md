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

# TransformXml

## XSLT Transform with Parameters

XSLT parameters are placeholders within an XSLT stylesheet that allow external values
to be passed into the transformation process at runtime.
Parameters are accessible within a stylesheet as normal variables, using the ```$name``` syntax,
provided they are declared using a top-level ```xsl:param``` element.
If there is no such declaration, the supplied parameter value is silently ignored.

### XSLT with Parameter Defined
Consider the following XML
```xml
<?xml version="1.0" encoding="UTF-8"?>
<data>
    <item>Some data</item>
</data>
```
and XSLT stylesheet
```xml
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:param name="customParam" select="'From XSLT'"/>
    <xsl:template match="/">
        <root>
            <message>
                Value Selected: <xsl:value-of select="$customParam"/>
            </message>
        </root>
    </xsl:template>
</xsl:stylesheet>
```
If the following parameter name ```customParam``` and parameter value ```From NIFI``` pair are added 
as a dynamic property, then the output would be
```xml
<?xml version="1.0" encoding="UTF-8"?>
<root>
    <message>
        Value Selected: From NIFI</message>
</root>
```
Note the  value of the ```customParam``` parameter became ```From NIFI``` even though
the declared ```xsl:param``` element had a default value of ```From XSLT```.

### XSLT without Parameter Defined 
If the XSLT stylesheet does not have a top-level ```xsl:param``` element declared
```xml
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/">
        <root>
            <message>
                Value Selected:
            </message>
        </root>
    </xsl:template>
</xsl:stylesheet>
```
even if the following parameter name ```customParam``` and parameter value ```From NIFI``` pair
are added as a dynamic property, they are ignored and the output would be
```xml
<?xml version="1.0" encoding="UTF-8"?>
<root>
 <message>
  Value Selected:
 </message>
</root>
```
In a case where the parameter is not declared in a ```xsl:param``` element, but attempted to be used in an XSLT stylesheet,
```xml
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/">
        <root>
            <message>
                Value Selected: <xsl:value-of select="$customParam"/>
            </message>
        </root>
    </xsl:template>
</xsl:stylesheet>
```
even if the following parameter name ```customParam``` and parameter value ```From NIFI``` pair are added as a dynamic property,
the transform will fail with an error message containing text ```Variable $customParam has not been declared```.

### XSLT Parameter Defined with Required Type
Starting in XSLT 2.0, an ```as``` attribute was added to parameters allowing for
specifying the type of parameter. The following are some of the more common types supported and what
valid values to use for each:

| Type Name         | Type Value                                                                                                               |
|:------------------|:-------------------------------------------------------------------------------------------------------------------------|
| ```xs:string```   | Any string value.                                                                                                        |
 | ```xs:boolean```  | A lowercase value of true or false.                                                                                      |
| ```xs:integer```  | A signed integer value.                                                                                                  |
| ```xs:float```    | A 32-bit IEEE single-precision floating-point.                                                                           |
| ```xs:double```   | A 64-bit IEEE single-precision floating-point.                                                                           |
| ```xs:date```     | A date in ISO 8601 format YYYY-MM-DD                                                                                     |
| ```xs:dateTime``` | A date time in ISO 8601 format: YYYY-MM-DDThh:mm:ss with optional uses of Z to represent UTC or an offset (e.g. -05:00). |
| ```xs:time```     | A time in format HH:mm:ss or HH:mm:ss.SSS with optional uses of Z to represent UTC or an offset (e.g. -05:00).           |
| ```xs:anyURI```   | A valid URI.                                                                                                             | 


The following XSLT stylesheet demonstrates the use of specifying types in an XSLT
and populating their values with dynamic properties.

Consider the following XSLT stylesheet
```xml
<xsl:stylesheet version="3.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xsl:param name="stringParam" as="xs:string" select="'From XSLT'"/>
    <xsl:param name="integerParam" as="xs:integer" select="0"/>
    <xsl:param name="floatParam" as="xs:float" select="0.0"/>
    <xsl:param name="doubleParam" as="xs:double" select="0.0"/>
    <xsl:param name="booleanParam" as="xs:boolean" select="false"/>
    <xsl:param name="dateParam" as="xs:date" select="xs:date('1970-01-01')"/>
    <xsl:param name="dateTimeParam" as="xs:dateTime" select="xs:dateTime('1970-01-01T00:00:00')"/>
    <xsl:param name="timeParam" as="xs:time" select="current-time()"/>
    <xsl:param name="utcTimeParam" as="xs:time" select="current-time()"/>
    <xsl:param name="offsetTimeParam" as="xs:time" select="current-time()"/>
    <xsl:param name="uriParam" as="xs:anyURI"/>
    <xsl:template match="/">
        <report>
            <message>Param of type xs:string value is <xsl:value-of select="$stringParam"/></message>
            <message>Param of type xs:integer value is <xsl:value-of select="$integerParam"/></message>
            <message>Param of type xs:float value is <xsl:value-of select="$floatParam"/></message>
            <message>Param of type xs:double value is <xsl:value-of select="$doubleParam"/></message>
            <message>Param of type xs:boolean value is <xsl:value-of select="$booleanParam"/></message>
            <message>Param of type xs:date value is <xsl:value-of select="$dateParam"/></message>
            <message>Param of type xs:dateTime value is <xsl:value-of select="$dateTimeParam"/></message>
            <message>Param of type xs:time value is <xsl:value-of select="$timeParam"/></message>
            <message>Param of type xs:time UTC value is <xsl:value-of select="$utcTimeParam"/></message>
            <message>Param of type xs:time offset value is <xsl:value-of select="$offsetTimeParam"/></message>
            <message>Param of type xs:anyURI <xsl:value-of select="$uriParam"/></message>
        </report>
    </xsl:template>
</xsl:stylesheet>
```
and the use of the XML from before

```xml
<?xml version="1.0" encoding="UTF-8"?>
<data>
    <item>Some data</item>
</data>
```

If the following parameter name and parameter value pairs are added as dynamic properties

| Parameter Name  | Parameter Value       |
|:----------------|:----------------------|
| stringParam     | From NIFI             |
| integerParam    | 100                   |
| floatParam      | 123.456               |
| doubleParam     | 12.78e-2              |
| booleanParam    | true                  |
| dateParam       | 2026-01-01            |
| dateTimeParam   | 2026-01-01T00:00:00   |
| timeParam       | 12:34:56.789          |
| utcTimeParam    | 12:34:56Z             |
| offsetTimeParam | 12:34:56-05:00        |
| uriParam        | http://from-nifi.com  |


then the resulting XML will be
```xml
<?xml version="1.0" encoding="UTF-8"?>
<report xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <message>Param of type xs:string value is From NIFI</message>
    <message>Param of type xs:integer value is 100</message>
    <message>Param of type xs:float value is 123.456</message>
    <message>Param of type xs:double value is 0.1278</message>
    <message>Param of type xs:boolean value is true</message>
    <message>Param of type xs:date value is 2026-01-01</message>
    <message>Param of type xs:dateTime value is 2026-01-01T00:00:00</message>
    <message>Param of type xs:time value is 12:34:56.789</message>
    <message>Param of type xs:time UTC value is 12:34:56Z</message>
    <message>Param of type xs:time offset value is 12:34:56-05:00</message>
    <message>Param of type xs:anyURI http://from-nifi.com</message>
</report>
```
Please note the "static" attribute added to the ```xsl:param``` element in 3.0
does not work with the dynamic properties in NIFI because as implied, static means 
a parameter value must be known at compile time of the XSLT while the dynamic
properties are supplied at run time of the actual transform.