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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.validation.Schema;
import javax.xml.validation.ValidatorHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

        assertEscapedCharacterReferenceLimitBehavior(new StandardDocumentProvider());
        assertEscapedCharacterReferenceLimitBehavior(new DocumentTypeAllowedStandardDocumentProvider());
    }

    @Test
    void testParseDocumentTypeDeclarationException() throws IOException {
        final StandardDocumentProvider provider = new StandardDocumentProvider();
        provider.setMaximumGeneralEntitySizeLimit(1024);
        provider.setMaximumTotalEntitySizeLimit(1024);

        try (final InputStream inputStream = ResourceProvider.getStandardDocumentDocType()) {
            assertParsingException(inputStream, provider);
        }
    }

    @Test
    void testParseExternalEntityException() throws IOException {
        final StandardDocumentProvider provider = new StandardDocumentProvider();
        provider.setMaximumGeneralEntitySizeLimit(1024);
        provider.setMaximumTotalEntitySizeLimit(1024);

        assertParsingException(provider);
    }

    @Test
    void testParseNamespaceAwareSchemaConfiguredExternalEntityException() throws IOException {
        when(schema.newValidatorHandler()).thenReturn(validatorHandler);

        final StandardDocumentProvider provider = new StandardDocumentProvider();
        provider.setNamespaceAware(true);
        provider.setSchema(schema);
        provider.setErrorHandler(errorHandler);
        provider.setMaximumGeneralEntitySizeLimit(1024);
        provider.setMaximumTotalEntitySizeLimit(1024);

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

    private void assertEscapedCharacterReferenceLimitBehavior(final StandardDocumentProvider provider) {
        provider.setMaximumGeneralEntitySizeLimit(64);

        ProcessingException processingException = assertThrows(ProcessingException.class,
                () -> provider.parse(getEscapedCharacterReferenceDocument(128)));
        assertInstanceOf(SAXParseException.class, processingException.getCause());

        provider.setMaximumGeneralEntitySizeLimit(null);

        Document document = provider.parse(getEscapedCharacterReferenceDocument(128));
        assertEquals(getEscapedCharacterReferenceValue(128), document.getDocumentElement().getElementsByTagName("subNode").item(0).getAttributes().getNamedItem("value").getNodeValue());

        provider.setMaximumTotalEntitySizeLimit(64);

        processingException = assertThrows(ProcessingException.class,
                () -> provider.parse(getEscapedCharacterReferenceDocument(128)));
        assertInstanceOf(SAXParseException.class, processingException.getCause());

        provider.setMaximumTotalEntitySizeLimit(null);

        document = provider.parse(getEscapedCharacterReferenceDocument(128));
        assertEquals(getEscapedCharacterReferenceValue(128), document.getDocumentElement().getElementsByTagName("subNode").item(0).getAttributes().getNamedItem("value").getNodeValue());
    }

    private InputStream getEscapedCharacterReferenceDocument(final int characterLength) {
        final String document = """
                <?xml version="1.0" encoding="UTF-8"?>
                <standard>
                    <subNode value="%s"/>
                </standard>
                """.formatted(getEscapedCharacterReferenceMarkup(characterLength));
        return new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8));
    }

    private String getEscapedCharacterReferenceMarkup(final int characterLength) {
        final String[] escapedCharacters = {"&lt;", "&gt;", "&amp;"};
        final StringBuilder builder = new StringBuilder(characterLength * 4);

        for (int i = 0; i < characterLength; i++) {
            builder.append(escapedCharacters[i % escapedCharacters.length]);
        }

        return builder.toString();
    }

    private String getEscapedCharacterReferenceValue(final int characterLength) {
        final char[] expandedCharacters = {'<', '>', '&'};
        final StringBuilder builder = new StringBuilder(characterLength);

        for (int i = 0; i < characterLength; i++) {
            builder.append(expandedCharacters[i % expandedCharacters.length]);
        }

        return builder.toString();
    }

    private static class DocumentTypeAllowedStandardDocumentProvider extends StandardDocumentProvider {
        @Override
        protected boolean isDisallowDocumentTypeDeclaration() {
            return false;
        }
    }
}
