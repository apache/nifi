<?xml version = "1.0" encoding = "UTF-8"?>
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
<xsl:stylesheet version = "1.0"
                xmlns:xsl = "http://www.w3.org/1999/XSL/Transform">
    <xsl:template match = "/">
        <html>
            <body>
                <h2>Employee</h2>
                <table border = "1">
                    <tr bgcolor = "pink">
                        <th>ID</th>
                        <th>First Name</th>
                        <th>Last Name</th>
                        <th>Nick Name</th>
                        <th>Salary</th>
                    </tr>

                    <xsl:for-each select = "class/employee">

                        <xsl:if test = "firstname = ''">
                            <xsl:message terminate = "yes">A first name field is empty.
                            </xsl:message>
                        </xsl:if>

                        <tr>
                            <td><xsl:value-of select = "@id"/></td>
                            <td><xsl:value-of select = "firstname"/></td>
                            <td><xsl:value-of select = "lastname"/></td>
                            <td><xsl:value-of select = "nickname"/></td>
                            <td><xsl:value-of select = "salary"/></td>
                        </tr>
                    </xsl:for-each>
                </table>
            </body>
        </html>
    </xsl:template>
</xsl:stylesheet>