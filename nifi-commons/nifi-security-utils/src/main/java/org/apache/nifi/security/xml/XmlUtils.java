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
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class XmlUtils {

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

        saxParserFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        saxParserFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

        SAXParser saxParser = saxParserFactory.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(contentHandler);

        return xmlReader;
    }
}
