<?xml version="1.0" encoding="ISO-8859-1"?>
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
<xsl:stylesheet version="2.0" 
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
	xmlns:xs="http://www.w3.org/2001/XMLSchema"
	xmlns:fn="fn"
	exclude-result-prefixes="xs fn">

	<xsl:param name="uuid_0" />
	<xsl:param name="uuid_1" />

	<xsl:output indent="yes" encoding="ISO-8859-1" />
	
	<!--  function getTokens handles these special cases:
	       Input file:
	       <data>"foo, bar",foo,"",bar,"""",foo "","" bar"</data>
	       
          Output file: 
      <token>foo, bar</token>
      <token>foo</token>
      <token/>
      <token>bar</token>
      <token>"</token>
      <token>foo "," bar</token>
    -->
    <xsl:function name="fn:getTokens" as="xs:string+">
        <xsl:param name="str" as="xs:string" />
        <xsl:analyze-string select="concat($str, ',')" regex='(("[^"]*")+|[^,]*),'> 
            <xsl:matching-substring>
                <xsl:sequence select ='replace(regex-group(1), "^""|""$|("")""", "$1")' />
            </xsl:matching-substring>
        </xsl:analyze-string>
    </xsl:function>

    <xsl:template match="data">
        <xsl:variable name="input" select="." />
        <xsl:variable name="rows" select="tokenize($input, '\r?\n')" />

        <!-- parse the header just for grins -->
        <xsl:variable name="header" select="remove($rows, 2)" />
        <xsl:variable name="h" select="tokenize($header, ',')" />

        <!-- parse and transform the data rows -->
        <test release="0.0" id="{$uuid_0}">
            <xsl:for-each select="remove($rows, 2)">
                <xsl:if test="string-length(.) > 0">
                    <xsl:variable name="v" select="fn:getTokens(.)" as="xs:string+" />
                    <event id="{$uuid_1}">
                        <token>
                            <xsl:value-of select="$v[1]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[2]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[3]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[4]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[5]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[6]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[7]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[8]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[9]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[10]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[11]" />
                        </token>
                        <token>
                            <xsl:value-of select="$v[12]" />
                        </token>
                    </event>
                </xsl:if>
            </xsl:for-each>
        </test>
    </xsl:template>
</xsl:stylesheet>