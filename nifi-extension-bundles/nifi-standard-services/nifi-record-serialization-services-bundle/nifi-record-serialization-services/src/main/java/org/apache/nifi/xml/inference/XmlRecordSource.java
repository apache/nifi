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
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLEventReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLEventReaderProvider;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.stream.StreamSource;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class XmlRecordSource implements RecordSource<XmlNode> {

    private final XMLEventReader xmlEventReader;
    private final String contentFieldName;
    private final boolean parseXmlAttributes;
    private final String attributePrefix;

    public XmlRecordSource(final InputStream in, final String contentFieldName, final boolean ignoreWrapper, final boolean parseXmlAttributes) throws IOException {
        this(in, contentFieldName, ignoreWrapper, parseXmlAttributes, null);
    }

    public XmlRecordSource(final InputStream in, final String contentFieldName, final boolean ignoreWrapper, final boolean parseXmlAttributes, final String attributePrefix) throws IOException {
        this.contentFieldName = contentFieldName;
        this.parseXmlAttributes = parseXmlAttributes;
        this.attributePrefix = attributePrefix;
        try {
            final XMLEventReaderProvider provider = new StandardXMLEventReaderProvider();
            xmlEventReader = provider.getEventReader(new StreamSource(in));

            if (ignoreWrapper) {
                readStartElement();
            }
        } catch (final ProcessingException | XMLStreamException e) {
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

        if (parseXmlAttributes) {
            addXmlAttributesToChildNodes(startElement, childNodes);
        }

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
            final String textContent = content.toString().trim();
            if (!textContent.equals("")) {
                childNodes.put(contentFieldName, new XmlTextNode(contentFieldName, textContent));
            }

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

    private void addXmlAttributesToChildNodes(StartElement startElement, Map<String, XmlNode> childNodes) {
        final Iterator<?> attributeIterator = startElement.getAttributes();
        while (attributeIterator.hasNext()) {
            final Attribute attribute = (Attribute) attributeIterator.next();
            final String rawName = attribute.getName().getLocalPart();
            final String fieldName = attributePrefix == null ? rawName : attributePrefix + rawName;
            childNodes.put(fieldName, new XmlTextNode(fieldName, attribute.getValue()));
        }
    }
}
