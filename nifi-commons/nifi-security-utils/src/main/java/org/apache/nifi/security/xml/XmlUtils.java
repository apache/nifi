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

import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class XmlUtils {

    // These features are used to disable processing external entities in the DocumentBuilderFactory to protect against XXE attacks
    private static final String DISALLOW_DOCTYPES = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String ALLOW_EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    private static final String ALLOW_EXTERNAL_PARAM_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    private static final String ALLOW_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

    public static XMLStreamReader createSafeReader(InputStream inputStream) throws XMLStreamException {
        if (inputStream == null) {
            throw new IllegalArgumentException("The provided input stream cannot be null");
        }
        return createSafeReader(new StreamSource(inputStream));
    }

    public static XMLStreamReader createSafeReader(StreamSource source) throws XMLStreamException {
        if (source == null) {
            throw new IllegalArgumentException("The provided source cannot be null");
        }

        XMLInputFactory xif = XMLInputFactory.newFactory();
        xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        return xif.createXMLStreamReader(source);
    }

    public static XMLReader createSafeSaxReader(SAXParserFactory saxParserFactory, ContentHandler contentHandler) throws SAXException, ParserConfigurationException {
        if (saxParserFactory == null) {
            throw new IllegalArgumentException("The provided SAX parser factory cannot be null");
        }

        if (contentHandler == null) {
            throw new IllegalArgumentException("The provided SAX content handler cannot be null");
        }

        SAXParser saxParser = saxParserFactory.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setFeature(DISALLOW_DOCTYPES, true);
        xmlReader.setFeature(ALLOW_EXTERNAL_GENERAL_ENTITIES, false);
        xmlReader.setFeature(ALLOW_EXTERNAL_PARAM_ENTITIES, false);
        xmlReader.setContentHandler(contentHandler);

        return xmlReader;
    }

    public static DocumentBuilder createSafeDocumentBuilder(Schema schema, boolean isNamespaceAware) throws ParserConfigurationException {
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setSchema(schema);
        docFactory.setNamespaceAware(isNamespaceAware);

        // Disable DTDs and external entities to protect against XXE
        docFactory.setAttribute(DISALLOW_DOCTYPES, true);
        docFactory.setAttribute(ALLOW_EXTERNAL_DTD, false);
        docFactory.setAttribute(ALLOW_EXTERNAL_GENERAL_ENTITIES, false);
        docFactory.setAttribute(ALLOW_EXTERNAL_PARAM_ENTITIES, false);
        docFactory.setXIncludeAware(false);
        docFactory.setExpandEntityReferences(false);

        return docFactory.newDocumentBuilder();
    }

    public static DocumentBuilder createSafeDocumentBuilder(Schema schema) throws ParserConfigurationException {
        return createSafeDocumentBuilder(schema, true);
    }

    public static DocumentBuilder createSafeDocumentBuilder(boolean isNamespaceAware) throws ParserConfigurationException {
        return createSafeDocumentBuilder(null, isNamespaceAware);
    }
}
