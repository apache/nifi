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
package org.apache.nifi.serialization.record.validation;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDefaultValidationError {

    @Test
    void testBuilderProducesCorrectFields() {
        final DefaultValidationError error = DefaultValidationError.builder()
                .fieldName("/root/name")
                .inputValue("badValue")
                .type(ValidationErrorType.MISSING_FIELD)
                .explanation("field is missing")
                .build();

        assertEquals(Optional.of("/root/name"), error.getFieldName());
        assertEquals(Optional.of("badValue"), error.getInputValue());
        assertEquals(ValidationErrorType.MISSING_FIELD, error.getType());
        assertEquals("field is missing", error.getExplanation());
    }

    @Test
    void testExplanationIsRequired() {
        final DefaultValidationError.Builder builder = DefaultValidationError.builder()
                .fieldName("/field")
                .type(ValidationErrorType.INVALID_FIELD);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void testTypeIsRequired() {
        final DefaultValidationError.Builder builder = DefaultValidationError.builder()
                .explanation("some explanation")
                .type(null);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void testDefaultTypeIsInvalidField() {
        final DefaultValidationError error = DefaultValidationError.builder()
                .explanation("some explanation")
                .build();

        assertEquals(ValidationErrorType.INVALID_FIELD, error.getType());
    }

    @Test
    void testOptionalFieldsDefaultToEmpty() {
        final DefaultValidationError error = DefaultValidationError.builder()
                .explanation("explanation only")
                .build();

        assertEquals(Optional.empty(), error.getFieldName());
        assertEquals(Optional.empty(), error.getInputValue());
    }

    @Test
    void testEqualsAndHashCode() {
        final DefaultValidationError error1 = DefaultValidationError.builder()
                .fieldName("/field")
                .inputValue(42)
                .type(ValidationErrorType.INVALID_FIELD)
                .explanation("bad value")
                .build();

        final DefaultValidationError error2 = DefaultValidationError.builder()
                .fieldName("/field")
                .inputValue(42)
                .type(ValidationErrorType.MISSING_FIELD)
                .explanation("bad value")
                .build();

        assertEquals(error1, error2);
        assertEquals(error1.hashCode(), error2.hashCode());
    }

    @Test
    void testNotEqualWhenExplanationDiffers() {
        final DefaultValidationError error1 = DefaultValidationError.builder()
                .fieldName("/field")
                .explanation("explanation A")
                .build();

        final DefaultValidationError error2 = DefaultValidationError.builder()
                .fieldName("/field")
                .explanation("explanation B")
                .build();

        assertNotEquals(error1, error2);
    }

    @Test
    void testNotEqualWhenFieldNameDiffers() {
        final DefaultValidationError error1 = DefaultValidationError.builder()
                .fieldName("/fieldA")
                .explanation("same")
                .build();

        final DefaultValidationError error2 = DefaultValidationError.builder()
                .fieldName("/fieldB")
                .explanation("same")
                .build();

        assertNotEquals(error1, error2);
    }

    @Test
    void testToStringContainsAllFields() {
        final DefaultValidationError error = DefaultValidationError.builder()
                .fieldName("/root/name")
                .inputValue("badValue")
                .type(ValidationErrorType.MISSING_FIELD)
                .explanation("field is missing")
                .build();

        final String result = error.toString();
        assertTrue(result.contains("field=/root/name"));
        assertTrue(result.contains("value=badValue"));
        assertTrue(result.contains("type=MISSING_FIELD"));
        assertTrue(result.contains("explanation=field is missing"));
    }
}
