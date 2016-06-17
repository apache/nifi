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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestEventTypeValidator {
    ValidationContext context;
    EventTypeValidator eventTypeValidator;

    @Before
    public void setUp() throws Exception {
        context = Mockito.mock(ValidationContext.class);
        eventTypeValidator = new EventTypeValidator();
    }

    @Test
    public void nullInputShouldProperlyFail() throws Exception {
        String subject = "subject";
        String input = null;
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals(null, result.getInput());
        assertEquals("Empty event types are not allowed.", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void emptyInputShouldProperlyFail() throws Exception {
        String subject = "subject";
        String input = "";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("", result.getInput());
        assertEquals("Empty event types are not allowed.", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void validEventTypesShouldProperlyValidate() throws Exception {
        String input = "  append, Create, CLOSE";
        String subject = "subject";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("  append, Create, CLOSE", result.getInput());
        assertEquals("", result.getExplanation());
        assertTrue(result.isValid());
    }

    @Test
    public void inputWithInvalidEventTypeShouldProperlyDisplayEventsInExplanation() throws Exception {
        String subject = "subject";
        String input = "append, CREATE, cllose, rename, metadata, unlink";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("append, CREATE, cllose, rename, metadata, unlink", result.getInput());
        assertEquals("The following are not valid event types: [cllose]", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void inputWithMultipleInvalidEventTypeShouldProperlyDisplayEventsInExplanation() throws Exception {
        String subject = "subject";
        String input = "append, CREATE, cllose, rename, metadata, unlink, unllink";
        ValidationResult result = eventTypeValidator.validate(subject, input, context);

        assertEquals("subject", result.getSubject());
        assertEquals("append, CREATE, cllose, rename, metadata, unlink, unllink", result.getInput());
        assertEquals("The following are not valid event types: [cllose, unllink]", result.getExplanation());
        assertFalse(result.isValid());
    }
}
