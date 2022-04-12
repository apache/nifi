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
import org.apache.nifi.xml.processing.ResourceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXParseException;

import javax.xml.validation.Schema;
import javax.xml.validation.ValidatorHandler;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardDocumentProviderTest {
    @Mock
    Schema schema;

    @Mock
    ValidatorHandler validatorHandler;

    @Mock
    ErrorHandler errorHandler;

    @Test
    void testNewDocument() {
        final StandardDocumentProvider provider = new StandardDocumentProvider();

        final Document document = provider.newDocument();

        assertNotNull(document);
    }

    @Test
    void testParseStandard() throws IOException {
        final StandardDocumentProvider provider = new StandardDocumentProvider();

        try (final InputStream inputStream = ResourceProvider.getStandardDocument()) {
            final Document document = provider.parse(inputStream);
            assertNotNull(document);
        }
    }

    @Test
    void testParseDocumentTypeDeclarationException() throws IOException {
        final StandardDocumentProvider provider = new StandardDocumentProvider();

        try (final InputStream inputStream = ResourceProvider.getStandardDocumentDocType()) {
            assertParsingException(inputStream, provider);
        }
    }

    @Test
    void testParseExternalEntityException() throws IOException {
        final StandardDocumentProvider provider = new StandardDocumentProvider();

        assertParsingException(provider);
    }

    @Test
    void testParseNamespaceAwareSchemaConfiguredExternalEntityException() throws IOException {
        when(schema.newValidatorHandler()).thenReturn(validatorHandler);

        final StandardDocumentProvider provider = new StandardDocumentProvider();
        provider.setNamespaceAware(true);
        provider.setSchema(schema);
        provider.setErrorHandler(errorHandler);

        assertParsingException(provider);
    }

    private void assertParsingException(final StandardDocumentProvider provider) throws IOException {
        try (final InputStream inputStream = ResourceProvider.getStandardDocumentDocTypeEntity()) {
            assertParsingException(inputStream, provider);
        }
    }

    private void assertParsingException(final InputStream inputStream, final StandardDocumentProvider provider) {
        final ProcessingException processingException = assertThrows(ProcessingException.class, () -> provider.parse(inputStream));
        assertInstanceOf(SAXParseException.class, processingException.getCause());
    }
}
