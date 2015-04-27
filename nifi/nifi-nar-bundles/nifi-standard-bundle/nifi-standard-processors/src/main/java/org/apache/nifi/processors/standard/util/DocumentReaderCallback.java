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
package org.apache.nifi.processors.standard.util;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.nifi.processor.io.InputStreamCallback;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class DocumentReaderCallback implements InputStreamCallback {

    private final boolean isNamespaceAware;
    private Document document;

    /**
     * Creates a new DocumentReaderCallback .
     *
     * @param isNamespaceAware Whether or not the parse should consider namespaces
     */
    public DocumentReaderCallback(boolean isNamespaceAware) {
        this.isNamespaceAware = isNamespaceAware;
    }

    @Override
    public void process(final InputStream stream) throws IOException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(isNamespaceAware);
            DocumentBuilder builder = factory.newDocumentBuilder();
            document = builder.parse(stream);
        } catch (ParserConfigurationException pce) {
            throw new IOException(pce.getLocalizedMessage(), pce);
        } catch (SAXException saxe) {
            throw new IOException(saxe.getLocalizedMessage(), saxe);
        }
    }

    /**
     * @return the document
     */
    public Document getDocument() {
        return document;
    }
}
