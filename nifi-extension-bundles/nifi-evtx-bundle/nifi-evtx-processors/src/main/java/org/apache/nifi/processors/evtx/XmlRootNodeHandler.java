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

package org.apache.nifi.processors.evtx;

import org.apache.nifi.processors.evtx.parser.bxml.RootNode;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class XmlRootNodeHandler implements RootNodeHandler {
    public static final String EVENTS = "Events";
    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newFactory();

    private final XMLStreamWriter xmlStreamWriter;
    private final XmlBxmlNodeVisitorFactory xmlBxmlNodeVisitorFactory;

    public XmlRootNodeHandler(final OutputStream outputStream) throws IOException {
        this(getXmlStreamWriter(new BufferedOutputStream(outputStream)), XmlBxmlNodeVisitor::new);
    }

    public XmlRootNodeHandler(final XMLStreamWriter xmlStreamWriter, final XmlBxmlNodeVisitorFactory xmlBxmlNodeVisitorFactory) throws IOException {
        this.xmlStreamWriter = xmlStreamWriter;
        this.xmlBxmlNodeVisitorFactory = xmlBxmlNodeVisitorFactory;
        try {
            this.xmlStreamWriter.writeStartDocument();
            try {
                this.xmlStreamWriter.writeStartElement(EVENTS);
            } catch (final XMLStreamException e) {
                this.xmlStreamWriter.close();
                throw e;
            }
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private static XMLStreamWriter getXmlStreamWriter(final OutputStream outputStream) throws IOException {
        try {
            return XML_OUTPUT_FACTORY.createXMLStreamWriter(outputStream, "UTF-8");
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handle(final RootNode rootNode) throws IOException {
        xmlBxmlNodeVisitorFactory.create(xmlStreamWriter, rootNode);
    }

    @Override
    public void close() throws IOException {
        try {
            xmlStreamWriter.writeEndElement();
        } catch (final XMLStreamException e) {
            throw new IOException(e);
        } finally {
            try {
                xmlStreamWriter.close();
            } catch (final XMLStreamException e) {
                throw new IOException(e);
            }
        }
    }
}
