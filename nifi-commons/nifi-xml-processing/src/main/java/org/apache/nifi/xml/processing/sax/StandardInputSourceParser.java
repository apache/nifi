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
package org.apache.nifi.xml.processing.sax;

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.ProcessingFeature;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.util.Objects;

/**
 * Standard implementation of Input Source Parser with secure processing enabled
 */
public class StandardInputSourceParser implements InputSourceParser {
    private boolean namespaceAware;

    /**
     * Set Namespace Aware status on SAXParserFactory
     *
     * @param namespaceAware Namespace Aware status
     */
    public void setNamespaceAware(final boolean namespaceAware) {
        this.namespaceAware = namespaceAware;
    }

    /**
     * Parse Input Source using Content Handler
     *
     * @param inputSource Input Source to be parsed
     * @param contentHandler Content Handler used during parsing
     */
    @Override
    public void parse(final InputSource inputSource, final ContentHandler contentHandler) {
        Objects.requireNonNull(inputSource, "InputSource required");
        Objects.requireNonNull(contentHandler, "ContentHandler required");

        try {
            parseInputSource(inputSource, contentHandler);
        } catch (final ParserConfigurationException|SAXException e) {
            throw new ProcessingException("Parsing failed", e);
        }
    }

    private void parseInputSource(final InputSource inputSource, final ContentHandler contentHandler) throws ParserConfigurationException, SAXException {
        final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        saxParserFactory.setNamespaceAware(namespaceAware);
        saxParserFactory.setXIncludeAware(false);

        saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, ProcessingFeature.SECURE_PROCESSING.isEnabled());
        saxParserFactory.setFeature(ProcessingFeature.DISALLOW_DOCTYPE_DECL.getFeature(), ProcessingFeature.DISALLOW_DOCTYPE_DECL.isEnabled());

        if (namespaceAware) {
            saxParserFactory.setFeature(ProcessingFeature.SAX_NAMESPACES.getFeature(), ProcessingFeature.SAX_NAMESPACES.isEnabled());
            saxParserFactory.setFeature(ProcessingFeature.SAX_NAMESPACE_PREFIXES.getFeature(), ProcessingFeature.SAX_NAMESPACE_PREFIXES.isEnabled());
        }

        final SAXParser parser = saxParserFactory.newSAXParser();

        final XMLReader reader = parser.getXMLReader();
        reader.setContentHandler(contentHandler);

        try {
            reader.parse(inputSource);
        } catch (final IOException e) {
            throw new ProcessingException("Parsing failed", e);
        }
    }
}
