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
package org.apache.nifi.xml.processing.validation;

import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.ResourceProvider;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StandardSchemaValidatorTest {
    private static final String SCHEMA_LANGUAGE = "http://www.w3.org/2001/XMLSchema";

    @Test
    void testValidate() throws SAXException, IOException {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(SCHEMA_LANGUAGE);
        final Schema schema;
        try (final InputStream inputStream = ResourceProvider.getStandardSchema()) {
            schema = schemaFactory.newSchema(new StreamSource(inputStream));
        }

        final StandardSchemaValidator validator = new StandardSchemaValidator();

        try (final InputStream inputStream = ResourceProvider.getStandardNamespaceDocument()) {
            validator.validate(schema, new StreamSource(inputStream));
        }
    }

    @Test
    void testValidateExternalEntityException() throws SAXException, IOException {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(SCHEMA_LANGUAGE);
        final Schema schema;
        try (final InputStream inputStream = ResourceProvider.getStandardSchema()) {
            schema = schemaFactory.newSchema(new StreamSource(inputStream));
        }

        final StandardSchemaValidator validator = new StandardSchemaValidator();

        try (final InputStream inputStream = ResourceProvider.getStandardNamespaceDocumentDocTypeEntity()) {
            final ProcessingException exception = assertThrows(ProcessingException.class, () -> validator.validate(schema, new StreamSource(inputStream)));
            final Throwable cause = exception.getCause();
            assertInstanceOf(SAXException.class, cause);
        }
    }
}
