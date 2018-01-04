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
package org.apache.nifi.toolkit.encryptconfig.util

import groovy.util.slurpersupport.GPathResult
import groovy.xml.XmlUtil
import org.apache.nifi.properties.SensitivePropertyProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class XmlEncryptor {

    protected static final String XML_DECLARATION_REGEX = /<\?xml version="1.0" encoding="UTF-8"\?>/
    protected static final ENCRYPTION_NONE = "none"
    protected static final ENCRYPTION_EMPTY = ""

    private static final Logger logger = LoggerFactory.getLogger(XmlEncryptor.class)

    protected SensitivePropertyProvider decryptionProvider
    protected SensitivePropertyProvider encryptionProvider

    XmlEncryptor(SensitivePropertyProvider encryptionProvider, SensitivePropertyProvider decryptionProvider) {
        this.decryptionProvider = decryptionProvider
        this.encryptionProvider = encryptionProvider
    }

    static boolean supportsFile(String filePath) {
        def doc
        try {
            String rawFileContents = loadXmlFile(filePath)
            doc = new XmlSlurper().parseText(rawFileContents)
        } catch (Throwable t) {
            return false
        }
        return doc != null
    }

    static String loadXmlFile(String xmlFilePath) throws IOException {
        File xmlFile = new File(xmlFilePath)
        if (ToolUtilities.canRead(xmlFile)) {
            try {
                String xmlContent = xmlFile.text
                return xmlContent
            } catch (RuntimeException e) {
                throw new IOException("Cannot load XML from ${xmlFilePath}", e)
            }
        } else {
            throw new IOException("File at ${xmlFilePath} must exist and be readable by user running this tool.")
        }
    }

    String decrypt(String encryptedXmlContent) {
        try {

            def doc = new XmlSlurper().parseText(encryptedXmlContent)

            List<GPathResult> encryptedNodes = new ArrayList<>()
            doc.depthFirst().forEachRemaining { GPathResult node ->
                if (node.@encryption != ENCRYPTION_NONE && node.@encryption != ENCRYPTION_EMPTY) {
                    encryptedNodes.add(node)
                }
            }

            if (encryptedNodes.isEmpty()) {
                logger.debug("No encrypted elements found in XML contents.")
                return encryptedXmlContent
            }

            if (decryptionProvider == null) {
                throw new IllegalStateException("Input XML is encrypted, but decryption capability is not enabled. " +
                        "Usually this means a decryption password / key was not provided to the tool.")
            }
            String supportedDecryptionScheme = decryptionProvider.getIdentifierKey()

            encryptedNodes.each { node ->
                logger.debug("Attempting to decrypt ${node.text()}")
                if (node.@encryption != supportedDecryptionScheme) {
                    throw new IllegalStateException("Decryption capability not supported by this tool. " +
                            "This tool supports ${supportedDecryptionScheme}, but this xml file contains " +
                            "${node.toString()} protected by ${node.@encryption}")
                }
                String decryptedValue = decryptionProvider.unprotect(node.text().trim())
                node.@encryption = ENCRYPTION_NONE
                node.replaceBody(decryptedValue)
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.debug("Updated XML content: ${updatedXml}")
            return updatedXml

        } catch (Exception e) {
            throw new RuntimeException("Cannot decrypt XML content", e)
        }
    }

    String encrypt(String plainXmlContent) {
        try {
            def doc = new XmlSlurper().parseText(plainXmlContent)

            List<GPathResult> nodesToEncrypt = new ArrayList<>()
            doc.depthFirst().forEachRemaining { GPathResult node ->
                if (node.@encryption == ENCRYPTION_NONE) {
                    nodesToEncrypt.add(node)
                }
            }

            if (nodesToEncrypt.isEmpty()) {
                logger.debug("No elements encrypted in XML.")
                return plainXmlContent
            }

            nodesToEncrypt.each { node ->
                String encryptedValue = this.encryptionProvider.protect(node.text().trim())
                node.@encryption = this.encryptionProvider.getIdentifierKey()
                node.replaceBody(encryptedValue)
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.debug("Updated XML content: ${updatedXml}")
            return updatedXml
        } catch (Exception e) {
            throw new RuntimeException("Cannot encrypt XML content", e)
        }
    }

    void writeXmlFile(String updatedXmlContent, String outputXmlPath, String inputXmlPath) throws IOException {
        File outputXmlFile = new File(outputXmlPath)
        if (ToolUtilities.isSafeToWrite(outputXmlFile)) {
            try {
                String finalXmlContent = serializeXmlContentAndPreserveFormatIfPossible(updatedXmlContent, inputXmlPath)
                outputXmlFile.text = finalXmlContent
            } catch (IOException e) {
                def msg = "Encountered an exception writing the protected values to ${outputXmlPath}"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The XML file at ${outputXmlPath} must be writable by the user running this tool")
        }
    }

    String serializeXmlContentAndPreserveFormatIfPossible(String updatedXmlContent, String inputXmlPath) {
        String finalXmlContent
        File inputXmlFile = new File(inputXmlPath)
        if (ToolUtilities.canRead(inputXmlFile)) {
            // Instead of just writing the XML content to a file,
            // this method attempts to maintain the structure of the original file
            finalXmlContent = serializeXmlContentAndPreserveFormat(updatedXmlContent, inputXmlFile).join("\n")
        } else {
            finalXmlContent = updatedXmlContent
        }
        return finalXmlContent
    }

    /**
     * Given an original XML file and updated XML content, create the lines for an updated, minimally altered, serialization.
     * Concrete classes extending this class must implement this method using specific knowledge of the XML document.
     *
     * @param finalXmlContent the xml content to serialize
     * @param inputXmlFile the original input xml file to use as a template for formatting the serialization
     * @return the lines of serialized finalXmlContent that are close in raw format to originalInputXmlFile
     */
    abstract List<String> serializeXmlContentAndPreserveFormat(String updatedXmlContent, File originalInputXmlFile)

    // TODO, replace the above abstract method with an implementation that works generically for any updated (encryption=."..") nodes
    // perhaps this could be done leveraging org.apache.commons.configuration2 which is capable of preserving comments, eg:

//    private List<String> serializeXmlContentAndPreserveFormat(String updatedXmlContent, File originalInputXmlFile) {
//        Configurations configurations = new Configurations()
//        try {
//            XMLConfiguration originalXmlConfiguration = configurations.xml(originalInputXmlFile)
//
//            // TO DO: override transformer: https://stackoverflow.com/a/16211353/2892227
//            // TO DO: update xml
//
//            OutputStream out = new ByteArrayOutputStream()
//            Writer writer = new GroovyPrintWriter(out)
//
//            originalXmlConfiguration.write(writer)
//
//            writer.flush()
//            List<String> lines = out.toString().split("\n")
//
//            return lines
//        } catch(Exception e) {
//            throw new RuntimeException("Error serializing xml.", e)
//        }
//    }
}
