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
package org.apache.nifi.security.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileLocator;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * For security reasons, this class overrides the Apache commons 'XMLConfiguration' class to disable processing of XML external entity (XXE) declarations.
 * This class should be used in all cases where an XML configuration file will be used by NiFi. It is currently used by the XMLFileLookupService.
 */
public class SafeXMLConfiguration extends XMLConfiguration {

    // Schema Language key for the parser
    private static final String JAXP_SCHEMA_LANGUAGE =
            "http://java.sun.com/xml/jaxp/properties/schemaLanguage";

    // Schema Language for the parser
    private static final String W3C_XML_SCHEMA =
            "http://www.w3.org/2001/XMLSchema";

    // These features are used to disable processing external entities in the DocumentBuilderFactory to protect against XXE attacks
    private static final String DISALLOW_DOCTYPES = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String ALLOW_EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    private static final String ALLOW_EXTERNAL_PARAM_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    private static final String ALLOW_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

    private static final String XXE_ERROR_MESSAGE = "XML configuration file contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.";

    public SafeXMLConfiguration() {
        super();
    }

    public SafeXMLConfiguration(HierarchicalConfiguration<ImmutableNode> c) {
        super(c);
    }

    @Override
    public void initFileLocator(FileLocator loc) {
        super.initFileLocator(loc);
    }

    /**
     * This overridden createDocumentBuilder() method sets the appropriate factory attributes to disable XXE parsing.
     *
     * @return Returns a safe DocumentBuilder
     * @throws ParserConfigurationException A configuration error
     */
    @Override
    public DocumentBuilder createDocumentBuilder() throws ParserConfigurationException {
        if (getDocumentBuilder() != null) {
            return getDocumentBuilder();
        }

        final DocumentBuilderFactory factory = DocumentBuilderFactory
                .newInstance();

        if (isValidating()) {
            factory.setValidating(true);
            if (isSchemaValidation()) {
                factory.setNamespaceAware(true);
                factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
            }
        }

        // Disable DTDs and external entities to protect against XXE
        factory.setAttribute(DISALLOW_DOCTYPES, true);
        factory.setAttribute(ALLOW_EXTERNAL_GENERAL_ENTITIES, false);
        factory.setAttribute(ALLOW_EXTERNAL_PARAM_ENTITIES, false);
        factory.setAttribute(ALLOW_EXTERNAL_DTD, false);

        final DocumentBuilder result;
        result = factory.newDocumentBuilder();
        result.setEntityResolver(super.getEntityResolver());

        if (isValidating()) {
            // register an error handler which detects validation errors
            result.setErrorHandler(new DefaultHandler() {
                @Override
                public void error(SAXParseException ex) throws SAXException {
                    throw ex;
                }
            });
        }

        return result;
    }

    @Override
    public void read(Reader in) throws ConfigurationException, IOException {
        delegateRead(() -> {
            super.read(in);
        });
    }

    @Override
    public void read(InputStream in) throws ConfigurationException, IOException {
        delegateRead(() -> {
            super.read(in);
        });
    }

    private void delegateRead(XMLReader superRead) throws ConfigurationException, IOException {
        try {
            superRead.read();
        } catch (ConfigurationException e) {
            if (isXXERelatedException(e)) {
                // Wrap this exception to tell the user their XML should not contain XXEs
                throw new ConfigurationException(XXE_ERROR_MESSAGE, e);
            } else {
                // Throw the original exception as it occurred for an unrelated reason
                throw e;
            }
        }
    }

    /**
     * Determine if the ConfigurationException was thrown because the XML configuration file contains an external entity (XXE).
     *
     * @param e A ConfigurationException that was thrown when parsing the XML configuration file.
     * @return true if the ConfigurationException was a result of attempting to parse an external entity (which is not allowed for security reasons). Returns false otherwise.
     */
    private boolean isXXERelatedException(ConfigurationException e) {
        return (e.getCause() instanceof SAXParseException && e.getCause().getMessage().contains("DOCTYPE is disallowed"));
    }

    @FunctionalInterface
    interface XMLReader {
        void read() throws ConfigurationException, IOException;
    }
}