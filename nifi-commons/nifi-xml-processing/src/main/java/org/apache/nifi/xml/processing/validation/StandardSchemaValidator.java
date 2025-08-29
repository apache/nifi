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
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;

import java.io.IOException;
import java.util.Objects;

/**
 * Standard implementation of XML Schema Validator with secure processing enabled
 */
public class StandardSchemaValidator implements SchemaValidator {
    private static final boolean SECURE_PROCESSING_ENABLED = true;

    /**
     * Validate Source using Schema
     *
     * @param schema Schema source for Validator
     * @param source Source to be validated
     */
    @Override
    public void validate(final Schema schema, final Source source) {
        Objects.requireNonNull(schema, "Schema required");
        Objects.requireNonNull(source, "Source required");

        final Validator validator = schema.newValidator();

        try {
            validator.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, SECURE_PROCESSING_ENABLED);
        } catch (final SAXException e) {
            throw new ProcessingException("Validator configuration failed", e);
        }

        try {
            validator.validate(source);
        } catch (final SAXException | IOException e) {
            throw new ProcessingException(String.format("Validation failed: %s", e.getMessage()), e);
        }
    }
}
