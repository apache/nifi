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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class XmlRootNodeHandlerTest {
    @Mock
    XMLStreamWriter xmlStreamWriter;

    @Mock
    XmlBxmlNodeVisitorFactory xmlBxmlNodeVisitorFactory;

    XmlRootNodeHandler xmlRootNodeHandler;

    @BeforeEach
    public void setup() throws IOException {
        xmlRootNodeHandler = new XmlRootNodeHandler(xmlStreamWriter, xmlBxmlNodeVisitorFactory);
    }

    @Test
    public void testConstructor() throws XMLStreamException {
        verify(xmlStreamWriter).writeStartDocument();
        verify(xmlStreamWriter).writeStartElement(XmlRootNodeHandler.EVENTS);
    }

    @Test
    public void testConstructorException() throws XMLStreamException {
        xmlStreamWriter = mock(XMLStreamWriter.class);
        doThrow(new XMLStreamException()).when(xmlStreamWriter).writeStartElement(XmlRootNodeHandler.EVENTS);
        assertThrows(IOException.class, () -> new XmlRootNodeHandler(xmlStreamWriter, xmlBxmlNodeVisitorFactory));
    }

    @Test
    public void testClose() throws IOException, XMLStreamException {
        xmlRootNodeHandler.close();
        verify(xmlStreamWriter).writeEndElement();
        verify(xmlStreamWriter).close();
    }

    @Test
    public void testCloseException() throws XMLStreamException {
        doThrow(new XMLStreamException()).when(xmlStreamWriter).close();
        assertThrows(IOException.class, () -> xmlRootNodeHandler.close());
    }
}
