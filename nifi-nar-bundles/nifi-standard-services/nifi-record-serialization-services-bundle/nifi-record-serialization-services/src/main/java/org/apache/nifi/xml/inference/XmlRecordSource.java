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
package org.apache.nifi.xml.inference;

import org.apache.nifi.schema.inference.RecordSource;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class XmlRecordSource implements RecordSource<XmlNode> {

    private final XMLEventReader xmlEventReader;

    public XmlRecordSource(final InputStream in, final boolean ignoreWrapper) throws IOException {
        try {
            final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

            // Avoid XXE Vulnerabilities
            xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            xmlInputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);

            xmlEventReader = xmlInputFactory.createXMLEventReader(in);

            if (ignoreWrapper) {
                readStartElement();
            }
        } catch (XMLStreamException e) {
            throw new IOException("Could not parse XML", e);
        }
    }

    @Override
    public XmlNode next() throws IOException {
        try {
            // Find a start element
            final StartElement startElement = readStartElement();
            if (startElement == null) {
                return null;
            }

            final XmlNode xmlNode = readNext(startElement);
            return xmlNode;
        } catch (final XMLStreamException xmle) {
            throw new IOException(xmle);
        }
    }

    private XmlNode readNext(final StartElement startElement) throws XMLStreamException, IOException {
        // Parse everything until we encounter the end element
        final StringBuilder content = new StringBuilder();
        final Map<String, XmlNode> childNodes = new LinkedHashMap<>();

        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isEndDocument()) {
                throw new EOFException("Expected to encounter End-of-Element tag for start tag '" + startElement.getName() + "'");
            }

            if (xmlEvent.isEndElement()) {
                break;
            }

            if (xmlEvent.isCharacters()) {
                final Characters characters = xmlEvent.asCharacters();
                if (!characters.isWhiteSpace()) {
                    content.append(characters.getData());
                }
            }

            if (xmlEvent.isStartElement()) {
                final StartElement childStartElement = xmlEvent.asStartElement();
                final XmlNode childNode = readNext(childStartElement);
                final String childName = childStartElement.getName().getLocalPart();

                final XmlNode existingNode = childNodes.get(childName);
                if (existingNode == null) {
                    childNodes.put(childName, childNode);
                } else if (existingNode.getNodeType() == XmlNodeType.ARRAY) {
                    ((XmlArrayNode) existingNode).addElement(childNode);
                } else {
                    final XmlArrayNode arrayNode = new XmlArrayNode(childStartElement.getName().getLocalPart());
                    arrayNode.addElement(existingNode);
                    arrayNode.addElement(childNode);
                    childNodes.put(childName, arrayNode);
                }
            }
        }

        final String nodeName = startElement.getName().getLocalPart();
        if (childNodes.isEmpty()) {
            return new XmlTextNode(nodeName, content.toString().trim());
        } else {
            return new XmlContainerNode(nodeName, childNodes);
        }
    }

    private StartElement readStartElement() throws XMLStreamException {
        while (xmlEventReader.hasNext()) {
            final XMLEvent xmlEvent = xmlEventReader.nextEvent();

            if (xmlEvent.isStartElement()) {
                final StartElement startElement = xmlEvent.asStartElement();
                return startElement;
            }
        }

        return null;
    }
}
