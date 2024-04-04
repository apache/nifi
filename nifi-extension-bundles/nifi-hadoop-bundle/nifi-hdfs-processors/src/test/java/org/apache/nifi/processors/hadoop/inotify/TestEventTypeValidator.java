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
package org.apache.nifi.processors.hadoop.inotify;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestEventTypeValidator {
    ValidationContext context;
    EventTypeValidator eventTypeValidator;

    @BeforeEach
    public void setUp() throws Exception {
        context = Mockito.mock(ValidationContext.class);
        eventTypeValidator = new EventTypeValidator();
    }

    @Test
    public void nullInputShouldProperlyFail() {
        String subject = "subject";
        String input = null;
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals(null, result.getInput());
        assertEquals("Empty event types are not allowed.", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void emptyInputShouldProperlyFail() {
        String subject = "subject";
        String input = "";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("", result.getInput());
        assertEquals("Empty event types are not allowed.", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void validEventTypesShouldProperlyValidate() {
        String input = "  append, Create, CLOSE";
        String subject = "subject";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("  append, Create, CLOSE", result.getInput());
        assertEquals("", result.getExplanation());
        assertTrue(result.isValid());
    }

    @Test
    public void inputWithInvalidEventTypeShouldProperlyDisplayEventsInExplanation() {
        String subject = "subject";
        String input = "append, CREATE, invalidValue1, rename, metadata, unlink";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("append, CREATE, invalidValue1, rename, metadata, unlink", result.getInput());
        assertEquals("The following are not valid event types: [invalidValue1]", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void inputWithMultipleInvalidEventTypeShouldProperlyDisplayEventsInExplanation() {
        String subject = "subject";
        String input = "append, CREATE, invalidValue1, rename, metadata, unlink, invalidValue2";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("append, CREATE, invalidValue1, rename, metadata, unlink, invalidValue2", result.getInput());
        assertEquals("The following are not valid event types: [invalidValue1, invalidValue2]", result.getExplanation());
        assertFalse(result.isValid());
    }
}
