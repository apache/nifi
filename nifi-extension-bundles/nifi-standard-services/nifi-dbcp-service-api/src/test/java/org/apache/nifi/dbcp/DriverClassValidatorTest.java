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
package org.apache.nifi.dbcp;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DriverClassValidatorTest {

    private static final String SUBJECT = "Database Driver Class";

    private static final String EMPTY = "";

    private static final String UNSUPPORTED_DRIVER = "org.h2.Driver";

    private static final String UNSUPPORTED_DRIVER_SPACED = String.format(" %s ", UNSUPPORTED_DRIVER);

    private static final String UNSUPPORTED_DRIVER_EXPRESSION = String.format("${attribute}%s", UNSUPPORTED_DRIVER);

    private static final String OTHER_DRIVER = "org.apache.nifi.Driver";

    private ValidationContext validationContext;

    private DriverClassValidator validator;

    @BeforeEach
    void setValidator() {
        validator = new DriverClassValidator();

        final MockProcessContext processContext = (MockProcessContext) TestRunners.newTestRunner(NoOpProcessor.class).getProcessContext();
        validationContext = new MockValidationContext(processContext);
    }

    @Test
    void testValidateEmpty() {
        final ValidationResult result = validator.validate(SUBJECT, EMPTY, validationContext);

        assertNotNull(result);
        assertFalse(result.isValid());
    }

    @Test
    void testValidateUnsupportedDriver() {
        final ValidationResult result = validator.validate(SUBJECT, UNSUPPORTED_DRIVER, validationContext);

        assertNotNull(result);
        assertFalse(result.isValid());
    }

    @Test
    void testValidateUnsupportedDriverExpressionLanguage() {
        final ValidationResult result = validator.validate(SUBJECT, UNSUPPORTED_DRIVER_EXPRESSION, validationContext);

        assertNotNull(result);
        assertFalse(result.isValid());
    }

    @Test
    void testValidateUnsupportedDriverSpaced() {
        final ValidationResult result = validator.validate(SUBJECT, UNSUPPORTED_DRIVER_SPACED, validationContext);

        assertNotNull(result);
        assertFalse(result.isValid());
    }

    @Test
    void testValidateSupportedDriver() {
        final ValidationResult result = validator.validate(SUBJECT, OTHER_DRIVER, validationContext);

        assertNotNull(result);
        assertTrue(result.isValid());
    }
}
