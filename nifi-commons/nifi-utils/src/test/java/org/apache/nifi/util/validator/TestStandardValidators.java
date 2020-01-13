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
package org.apache.nifi.util.validator;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestStandardValidators {

    @Test
    public void testNonBlankValidator() {
        Validator val = StandardValidators.NON_BLANK_VALIDATOR;
        ValidationContext vc = mock(ValidationContext.class);
        ValidationResult vr = val.validate("foo", "", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "    ", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "    h", vc);
        assertTrue(vr.isValid());
    }

    @Test
    public void testNonEmptyELValidator() {
        Validator val = StandardValidators.NON_EMPTY_EL_VALIDATOR;
        ValidationContext vc = mock(ValidationContext.class);
        Mockito.when(vc.isExpressionLanguageSupported("foo")).thenReturn(true);

        ValidationResult vr = val.validate("foo", "", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "    h", vc);
        assertTrue(vr.isValid());

        Mockito.when(vc.isExpressionLanguagePresent("${test}")).thenReturn(true);
        vr = val.validate("foo", "${test}", vc);
        assertTrue(vr.isValid());

        vr = val.validate("foo", "${test", vc);
        assertTrue(vr.isValid());
    }

    @Test
    public void testHostnamePortListValidator() {
        Validator val = StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR;
        ValidationContext vc = mock(ValidationContext.class);
        Mockito.when(vc.isExpressionLanguageSupported("foo")).thenReturn(true);

        ValidationResult vr = val.validate("foo", "", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "localhost", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "test:0", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "test:65536", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "test:6666,localhost", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "test:65535", vc);
        assertTrue(vr.isValid());

        vr = val.validate("foo", "test:65535,localhost:666,127.0.0.1:8989", vc);
        assertTrue(vr.isValid());

        Mockito.when(vc.isExpressionLanguagePresent("${test}")).thenReturn(true);
        vr = val.validate("foo", "${test}", vc);
        assertTrue(vr.isValid());

        vr = val.validate("foo", "${test", vc);
        assertFalse(vr.isValid());
    }

    @Test
    public void testTimePeriodValidator() {
        Validator val = StandardValidators.createTimePeriodValidator(1L, TimeUnit.SECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);

        vr = val.validate("TimePeriodTest", "0 sense made", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", null, validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "0 secs", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999 millis", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999999999 nanos", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "1 sec", validationContext);
        assertTrue(vr.isValid());
    }

    @Test
    public void testDataSizeBoundsValidator() {
        Validator val = StandardValidators.createDataSizeBoundsValidator(100, 1000);
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        vr = val.validate("DataSizeBounds", "5 GB", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "0 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "99 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "100 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "999 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "1000 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "1001 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "water", validationContext);
        assertFalse(vr.isValid());
    }

    @Test
    public void testListValidator() {
        Validator val = StandardValidators.createListValidator(true, false, StandardValidators.NON_EMPTY_VALIDATOR);
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);

        vr = val.validate("List", null, validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "", validationContext);
        assertFalse(vr.isValid());

        // Whitespace will be trimmed
        vr = val.validate("List", " ", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "1", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("List", "1,2,3", validationContext);
        assertTrue(vr.isValid());

        // The parser will not bother with whitespace after the last comma
        vr = val.validate("List", "a,", validationContext);
        assertTrue(vr.isValid());

        // However it will bother if there is an empty element in the list (two commas in a row, e.g.)
        vr = val.validate("List", "a,,c", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "a,  ,c, ", validationContext);
        assertFalse(vr.isValid());

        // Try without trim and use a non-blank validator instead of a non-empty one
        val = StandardValidators.createListValidator(false, true, StandardValidators.NON_BLANK_VALIDATOR);

        vr = val.validate("List", null, validationContext);
        assertFalse(vr.isValid());

        // Validator will ignore empty entries
        vr = val.validate("List", "", validationContext);
        assertTrue(vr.isValid());

        // Whitespace will not be trimmed, but it is still invalid because a non-blank validator is used
        vr = val.validate("List", " ", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "a,,c", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("List", "a,  ,c, ", validationContext);
        assertFalse(vr.isValid());

        // Try without trim and use a non-empty validator
        val = StandardValidators.createListValidator(false, false, StandardValidators.NON_EMPTY_VALIDATOR);

        vr = val.validate("List", null, validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "", validationContext);
        assertFalse(vr.isValid());

        // Whitespace will not be trimmed
        vr = val.validate("List", " ", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("List", "a,  ,c, ", validationContext);
        assertTrue(vr.isValid());

        // Try with trim and use a boolean validator
        val = StandardValidators.createListValidator(true, true, StandardValidators.BOOLEAN_VALIDATOR);
        vr = val.validate("List", "notbool", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "    notbool \n   ", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("List", "true", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("List", "    true   \n   ", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("List", " , false,  true,\n", validationContext);
        assertTrue(vr.isValid());
    }

    @Test
    public void testCreateURLorFileValidator() {
        Validator val = StandardValidators.createURLorFileValidator();
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);

        vr = val.validate("URLorFile", null, validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("URLorFile", "", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("URLorFile", "http://nifi.apache.org", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("URLorFile", "http//nifi.apache.org", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("URLorFile", "nifi.apache.org", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("URLorFile", "src/test/resources/this_file_exists.txt", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("URLorFile", "src/test/resources/this_file_does_not_exist.txt", validationContext);
        assertFalse(vr.isValid());

    }

    @Test
    public void testiso8061InstantValidator() {
        Validator val = StandardValidators.ISO8061_INSTANT_VALIDATOR;
        ValidationContext vc = mock(ValidationContext.class);
        ValidationResult vr = val.validate("foo", "", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "2016-01-01T01:01:01.000-0100", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "2016-01-01T01:01:01.000Z", vc);
        assertTrue(vr.isValid());
    }

    @Test
    public void testURIListValidator() {
        Validator val = StandardValidators.URI_LIST_VALIDATOR;
        ValidationContext vc = mock(ValidationContext.class);
        ValidationResult vr = val.validate("foo", null, vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "/no_scheme", vc);
        assertTrue(vr.isValid());

        vr = val.validate("foo", "http://localhost 8080, https://host2:8080 ", vc);
        assertFalse(vr.isValid());

        vr = val.validate("foo", "http://localhost , https://host2:8080 ", vc);
        assertTrue(vr.isValid());
    }
}
