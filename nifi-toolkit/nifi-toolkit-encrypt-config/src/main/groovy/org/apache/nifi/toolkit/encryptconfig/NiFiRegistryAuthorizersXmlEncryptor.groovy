/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.encryptconfig

import groovy.xml.XmlUtil
import org.apache.nifi.properties.SensitivePropertyProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXException

class NiFiRegistryAuthorizersXmlEncryptor extends XmlEncryptor {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryAuthorizersXmlEncryptor.class)

    private static final String LDAP_USER_GROUP_PROVIDER_CLASS = "org.apache.nifi.registry.security.ldap.tenants.LdapUserGroupProvider"
    private static final String LDAP_USER_GROUP_PROVIDER_REGEX =
            /(?s)<userGroupProvider>(?:(?!<userGroupProvider>).)*?<class>\s*org\.apache\.nifi\.registry\.security\.ldap\.tenants\.LdapUserGroupProvider.*?<\/userGroupProvider>/
    /* Explanation of LDAP_USER_GROUP_PROVIDER_REGEX:
     *   (?s)                             -> single-line mode (i.e., `.` in regex matches newlines)
     *   <userGroupProvider>              -> find occurrence of `<userGroupProvider>` literally (case-sensitive)
     *   (?: ... )                        -> group but do not capture submatch
     *   (?! ... )                        -> negative lookahead
     *   (?:(?!<userGroupProvider>).)*?   -> find everything until a new `<userGroupProvider>` starts. This is for not selecting multiple userGroupProviders in one match
     *   <class>                          -> find occurrence of `<class>` literally (case-sensitive)
     *   \s*                              -> find any whitespace
     *   org\.apache\.nifi\.registry\.security\.ldap\.tenants\.LdapUserGroupProvider
     *                                    -> find occurrence of `org.apache.nifi.registry.security.ldap.tenants.LdapUserGroupProvider` literally (case-sensitive)
     *   .*?</userGroupProvider>          -> find everything as needed up until and including occurrence of '</userGroupProvider>'
     */

    NiFiRegistryAuthorizersXmlEncryptor(SensitivePropertyProvider encryptionProvider, SensitivePropertyProvider decryptionProvider) {
        super(encryptionProvider, decryptionProvider)
    }

    @Override
    String encrypt(String plainXmlContent) {
        // First, mark the XML nodes to encrypt that are specific to authorizers.xml by adding an attribute encryption="none"
        String markedXmlContent
        try {
            def doc = new XmlSlurper().parseText(plainXmlContent)
            // Find the provider element by class even if it has been renamed
            def passwords = doc.userGroupProvider.find { it.'class' as String == LDAP_USER_GROUP_PROVIDER_CLASS }
                    .property.findAll {
                // Only operate on populated password properties
                it.@name =~ "Password" && it.text()
            }

            if (passwords.isEmpty()) {
                logger.debug("No populated password property elements found in authorizers.xml")
                return plainXmlContent
            }

            passwords.each { password ->
                logger.debug("Marking ${password.name()} to be encrypted")
                password.@encryption = ENCRYPTION_NONE
            }

            // Does not preserve whitespace formatting or comments
            // TODO: Switch to XmlParser & XmlNodePrinter to maintain "empty" element structure
            markedXmlContent = XmlUtil.serialize(doc)
        } catch (Exception e) {
            logger.debug("Encountered exception", e)
            throw new RuntimeException("Cannot encrypt login identity providers XML content.", e)
        }

        // Now, return the results of the base implementation, which will encrypt the body of any node with an encryption="none" attribute
        return super.encrypt(markedXmlContent)
    }

    List<String> serializeXmlContentAndPreserveFormat(String xmlContent, File originalInputXmlFile) {
        // Find the provider element of the new XML in the file contents
        String fileContents = originalInputXmlFile.text
        try {
            def parsedXml = new XmlSlurper().parseText(xmlContent)
            def provider = parsedXml.userGroupProvider.find { it.'class' as String == LDAP_USER_GROUP_PROVIDER_CLASS }
            if (provider) {
                def serializedProvider = new XmlUtil().serialize(provider)
                // Remove XML declaration from top
                serializedProvider = serializedProvider.replaceFirst(XML_DECLARATION_REGEX, "")
                fileContents = fileContents.replaceFirst(LDAP_USER_GROUP_PROVIDER_REGEX, serializedProvider)
                return fileContents.split("\n")
            } else {
                throw new SAXException("No ldap-user-group-provider element found")
            }
        } catch (SAXException e) {
            logger.error("No provider element with class {} found in XML content; " +
                    "the file could be empty or the element may be missing or commented out", LDAP_USER_GROUP_PROVIDER_CLASS)
            return fileContents.split("\n")
        }
    }

}
