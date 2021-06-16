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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class XmlRootNodeHandlerTest {
    @Mock
    XMLStreamWriter xmlStreamWriter;

    @Mock
    XmlBxmlNodeVisitorFactory xmlBxmlNodeVisitorFactory;

    XmlRootNodeHandler xmlRootNodeHandler;

    @Before
    public void setup() throws IOException {
        xmlRootNodeHandler = new XmlRootNodeHandler(xmlStreamWriter, xmlBxmlNodeVisitorFactory);
    }

    @Test
    public void testConstructor() throws XMLStreamException {
        verify(xmlStreamWriter).writeStartDocument();
        verify(xmlStreamWriter).writeStartElement(XmlRootNodeHandler.EVENTS);
    }

    @Test(expected = IOException.class)
    public void testConstructorException() throws XMLStreamException, IOException {
        xmlStreamWriter = mock(XMLStreamWriter.class);
        doThrow(new XMLStreamException()).when(xmlStreamWriter).writeStartElement(XmlRootNodeHandler.EVENTS);
        new XmlRootNodeHandler(xmlStreamWriter, xmlBxmlNodeVisitorFactory);
    }

    @Test
    public void testClose() throws IOException, XMLStreamException {
        xmlRootNodeHandler.close();
        verify(xmlStreamWriter).writeEndElement();
        verify(xmlStreamWriter).close();
    }

    @Test(expected = IOException.class)
    public void testCloseException() throws IOException, XMLStreamException {
        doThrow(new XMLStreamException()).when(xmlStreamWriter).close();
        xmlRootNodeHandler.close();
    }
}
