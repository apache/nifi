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
package org.apache.nifi.xml.processing.parsers;

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.ProcessingFeature;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Standard implementation of Document Provider with secure processing enabled
 */
public class StandardDocumentProvider implements DocumentProvider {
    private boolean namespaceAware;

    private Schema schema;

    private ErrorHandler errorHandler;

    /**
     * Set Error Handler
     *
     * @param errorHandler Error Handler
     */
    public void setErrorHandler(final ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    /**
     * Set Namespace Aware status on DocumentBuilderFactory
     *
     * @param namespaceAware Namespace Awareness
     */
    public void setNamespaceAware(final boolean namespaceAware) {
        this.namespaceAware = namespaceAware;
    }

    /**
     * Set Namespace Aware status on DocumentBuilderFactory
     *
     * @param schema Schema for validation or null to disable validation
     */
    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public Document newDocument() {
        final DocumentBuilderFactory documentBuilderFactory = getDocumentBuilderFactory();

        try {
            documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, ProcessingFeature.SECURE_PROCESSING.isEnabled());
            documentBuilderFactory.setFeature(ProcessingFeature.DISALLOW_DOCTYPE_DECL.getFeature(), ProcessingFeature.DISALLOW_DOCTYPE_DECL.isEnabled());

            final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            documentBuilder.setErrorHandler(errorHandler);

            return documentBuilder.newDocument();
        } catch (final ParserConfigurationException e) {
            throw new ProcessingException("Configuration failed", e);
        }
    }

    /**
     * Build and return DocumentBuilder
     *
     * @return DocumentBuilder configured using provided properties
     */
    @Override
    public Document parse(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "InputStream required");
        final DocumentBuilderFactory documentBuilderFactory = getDocumentBuilderFactory();

        try {
            documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, ProcessingFeature.SECURE_PROCESSING.isEnabled());
            documentBuilderFactory.setFeature(ProcessingFeature.DISALLOW_DOCTYPE_DECL.getFeature(), isDisallowDocumentTypeDeclaration());

            final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            documentBuilder.setErrorHandler(errorHandler);

            return documentBuilder.parse(inputStream);
        } catch (final ParserConfigurationException|SAXException|IOException e) {
            throw new ProcessingException("Parsing failed", e);
        }
    }

    protected boolean isDisallowDocumentTypeDeclaration() {
        return ProcessingFeature.DISALLOW_DOCTYPE_DECL.isEnabled();
    }

    private DocumentBuilderFactory getDocumentBuilderFactory() {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

        documentBuilderFactory.setSchema(schema);
        documentBuilderFactory.setNamespaceAware(namespaceAware);

        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);

        return documentBuilderFactory;
    }
}
