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
package org.apache.nifi.processor;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.JsonValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JsonValidatorTest {

    private static final String JSON_PROPERTY = "JSONProperty";

    @Mock
    private ValidationContext context;

    private final Validator validator = JsonValidator.INSTANCE;

    @Test
    void testNullBlank() {
        assertInvalid(null);
        assertInvalid("\r\n");
    }

    @Test
    void testObject() {
        final String input = """
                {
                  "label": "Example, User"
                }
                """;

        assertValid(input);
    }

    @Test
    void testObjectNested() {
        final String input = """
                {
                  "label": "Example, User",
                  "contact": {
                    "phone": 5550100,
                    "email": "user@example.com"
                  }
                }
                """;

        assertValid(input);
    }

    @Test
    void testObjectArray() {
        final String input = """
                {
                  "fullName": "Doe, Jane",
                  "age": 42,
                  "tags": [
                    "tag-a",
                    "tag-b",
                    "tag-c"
                  ]
                }
                """;

        assertValid(input);
    }

    @Test
    void testArray() {
        final String input = """
                [
                  "first",
                  "second",
                  "third"
                ]
                """;

        assertValid(input);
    }

    @Test
    void testEmptyObject() {
        final String input = "{}";

        assertValid(input);
    }

    @Test
    void testArrayWithLeadingWhitespace() {
        final String input = """
                \t[1,2]
                """;

        assertValid(input);
    }

    @Test
    void testScalarNumberInvalid() {
        final String input = "42";

        assertInvalid(input);
    }

    @Test
    void testObjectTrailingArrayInvalid() {
        final String input = """
                {
                  "label": "Example, User"
                }
                []
                """;

        assertInvalid(input);
    }

    @Test
    void testMalformedObjectInvalid() {
        final String input = """
                {
                  "label": "Example, User"
                """;

        assertInvalid(input);
    }

    @Test
    void testMalformedArrayInvalid() {
        final String input = "[1,2,3";

        assertInvalid(input);
    }

    @Test
    void testExpressionLanguageValid() {
        final String input = "${expression}";

        when(context.isExpressionLanguageSupported(eq(JSON_PROPERTY))).thenReturn(true);
        when(context.isExpressionLanguagePresent(eq(input))).thenReturn(true);

        final ValidationResult result = validator.validate(JSON_PROPERTY, input, context);
        assertTrue(result.isValid());

        assertTrue(result.getExplanation().contains("Expression Language"));
    }

    private void assertValid(final String input) {
        assertTrue(validator.validate(JSON_PROPERTY, input, context).isValid());
    }

    private void assertInvalid(final String input) {
        assertFalse(validator.validate(JSON_PROPERTY, input, context).isValid());
    }
}
