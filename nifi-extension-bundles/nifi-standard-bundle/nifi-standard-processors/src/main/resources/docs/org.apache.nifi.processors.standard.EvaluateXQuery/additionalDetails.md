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

# EvaluateXQuery

**Examples:**

This processor produces one attribute or FlowFile per XQueryResult. If only one attribute or FlowFile is desired, the
following examples demonstrate how this can be achieved using the XQuery language. The examples below reference the
following sample XML:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="foo.xsl"?>
<ns:fruitbasket xmlns:ns="http://namespace/1">
    <fruit taste="crisp">           <!-- Apples are my favorite-->
        <name>apple</name>
        <color>red</color>
    </fruit>
    <fruit>
        <name>apple</name>
        <color>green</color>
    </fruit>
    <fruit>
        <name>banana</name>
        <color>yellow</color>
    </fruit>
    <fruit taste="sweet">
        <name>orange</name>
        <color>orange</color>
    </fruit>
    <fruit>
        <name>blueberry</name>
        <color>blue</color>
    </fruit>
    <fruit taste="tart">
        <name>raspberry</name>
        <color>red</color>
    </fruit>
    <fruit>
        <name>none</name>
        <color/>
    </fruit>
</ns:fruitbasket>
```

* XQuery to return all "fruit" nodes individually (7 Results):
    * //fruit]

* XQuery to return only the first "fruit" node (1 Result):
    * //fruit\[1\]

* XQuery to return only the last "fruit" node (1 Result):
    * //fruit\[count(//fruit)\]

* XQuery to return all "fruit" nodes, wrapped in a "basket" tag (1 Result):
    * <basket>{//fruit}</basket>

* XQuery to return all "fruit" names individually (7 Results):
    * //fruit/text()

* XQuery to return only the first "fruit" name (1 Result):
    * //fruit\[1\]/text()

* XQuery to return only the last "fruit" name (1 Result):
    * //fruit\[count(//fruit)\]/text()

* XQuery to return all "fruit" names as a comma separated list (1 Result):
    * string-join((for \$x in //fruit return \$x/name/text()), ', ')

* XQuery to return all "fruit" colors and names as a comma separated list (1 Result):
    * string-join((for \$y in (for \$x in //fruit return string-join((\$x/color/text() , \$x/name/text()), ' ')) return
      \$y), ', ')

* XQuery to return all "fruit" colors and names as a comma separated list (1 Result):
    * string-join((for \$y in (for \$x in //fruit return string-join((\$x/color/text() , \$x/name/text()), ' ')) return
      \$y), ', ')

* XQuery to return all "fruit" colors and names as a new line separated list (1 Result):
    * string-join((for \$y in (for \$x in //fruit return string-join((\$x/color/text() , \$x/name/text()), ' ')) return
      \$y), '\\n')