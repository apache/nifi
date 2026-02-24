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

package org.apache.nifi.csv;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCSVValidators {

    /*** SingleCharValidator **/
    @Test
    public void testSingleCharNullValue() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", null, mockContext);
        assertEquals("Input is null for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testSingleCharEmptyValue() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "", mockContext);
        assertEquals("Value must be exactly 1 character but was 0 in length", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testSingleCharTab() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "\\t", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testSingleCharIllegalChar() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "\\r", mockContext);
        assertEquals("\\r is not a valid character for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testSingleCharExpressionLanguage() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        Mockito.when(mockContext.isExpressionLanguageSupported(Mockito.any())).thenReturn(true);
        Mockito.when(mockContext.isExpressionLanguagePresent(Mockito.any())).thenReturn(true);
        final ValidationResult result = validator.validate("EscapeChar", "${csv.escape}", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testSingleCharGoodChar() {
        final Validator validator = CSVValidators.SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "'", mockContext);
        assertTrue(result.isValid());
    }

    /*** Empty Or SingleCharValidator **/
    @Test
    public void testEmptySingleCharNullValue() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", null, mockContext);
        assertEquals("Input is null for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testEmptySingleCharTab() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "\\t", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testEmptySingleCharIllegalChar() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "\\r", mockContext);
        assertEquals("\\r is not a valid character for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testEmptySingleCharExpressionLanguage() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        Mockito.when(mockContext.isExpressionLanguageSupported(Mockito.any())).thenReturn(true);
        Mockito.when(mockContext.isExpressionLanguagePresent(Mockito.any())).thenReturn(true);
        final ValidationResult result = validator.validate("EscapeChar", "${csv.escape}", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testEmptySingleCharGoodChar() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "'", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testEmptySingleCharEmptyChar() {
        final Validator validator = CSVValidators.EMPTY_OR_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("EscapeChar", "", mockContext);
        assertTrue(result.isValid());
    }

    /*** Unescaped SingleCharValidator **/

    @Test
    public void testUnEscapedSingleCharNullValue() {
        final Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("Delimiter", null, mockContext);
        assertEquals("Input is null for this property", result.getExplanation());
        assertFalse(result.isValid());

    }

    @Test
    public void testUnescapedSingleCharUnicodeChar() {
        final Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("Delimiter", "\\u0001", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testUnescapedSingleCharGoodChar() {
        final Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        final ValidationResult result = validator.validate("Delimiter", ",", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testUnescapedSingleCharExpressionLanguage() {
        final Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        final ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        Mockito.when(mockContext.isExpressionLanguageSupported(Mockito.any())).thenReturn(true);
        Mockito.when(mockContext.isExpressionLanguagePresent(Mockito.any())).thenReturn(true);
        final ValidationResult result = validator.validate("Delimiter", "${csv.delimiter}", mockContext);
        assertTrue(result.isValid());
    }

}
