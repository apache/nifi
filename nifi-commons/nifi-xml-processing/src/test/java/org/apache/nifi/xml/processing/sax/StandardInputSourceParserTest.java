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
package org.apache.nifi.xml.processing.sax;

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.ResourceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class StandardInputSourceParserTest {
    @Mock
    ContentHandler contentHandler;

    @Test
    void testParseStandard() throws IOException {
        final StandardInputSourceParser parser = new StandardInputSourceParser();

        try (final InputStream inputStream = ResourceProvider.getStandardDocument()) {
            parser.parse(new InputSource(inputStream), contentHandler);
        }
    }

    @Test
    void testParseDocumentTypeDeclarationException() throws IOException {
        final StandardInputSourceParser parser = new StandardInputSourceParser();

        try (final InputStream inputStream = ResourceProvider.getStandardDocumentDocType()) {
            assertParsingException(inputStream, parser);
        }
    }

    @Test
    void testParseExternalEntityException() throws IOException {
        final StandardInputSourceParser parser = new StandardInputSourceParser();

        assertParsingException(parser);
    }

    @Test
    void testParseNamespaceAwareExternalEntityException() throws IOException {
        final StandardInputSourceParser parser = new StandardInputSourceParser();

        parser.setNamespaceAware(true);

        assertParsingException(parser);
    }

    private void assertParsingException(final StandardInputSourceParser parser) throws IOException {
        try (final InputStream inputStream = ResourceProvider.getStandardDocumentDocTypeEntity()) {
            assertParsingException(inputStream, parser);
        }
    }

    private void assertParsingException(final InputStream inputStream, final StandardInputSourceParser parser) {
        final ProcessingException processingException = assertThrows(ProcessingException.class, () -> parser.parse(new InputSource(inputStream), new DefaultHandler()));
        assertInstanceOf(SAXParseException.class, processingException.getCause());
    }
}
