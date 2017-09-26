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
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestCSVValidators {

    /*** SingleCharValidator **/
    @Test
    public void testSingleCharNullValue() {

        CSVValidators.SingleCharacterValidator validator = new CSVValidators.SingleCharacterValidator();
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("EscapeChar", null, mockContext);
        assertEquals("Input is null for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testSingleCharTab() {
        CSVValidators.SingleCharacterValidator validator = new CSVValidators.SingleCharacterValidator();
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("EscapeChar", "\\t", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testSingleCharIllegalChar() {
        CSVValidators.SingleCharacterValidator validator = new CSVValidators.SingleCharacterValidator();
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("EscapeChar", "\\r", mockContext);
        assertEquals("\\r is not a valid character for this property", result.getExplanation());
        assertFalse(result.isValid());
    }

    @Test
    public void testSingleCharGoodChar() {
        CSVValidators.SingleCharacterValidator validator = new CSVValidators.SingleCharacterValidator();
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("EscapeChar", "'", mockContext);
        assertTrue(result.isValid());
    }


    /*** Unescaped SingleCharValidator **/

    @Test
    public void testUnEscapedSingleCharNullValue() {
        Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("Delimiter", null, mockContext);
        assertEquals("Input is null for this property", result.getExplanation());
        assertFalse(result.isValid());

    }

    @Test
    public void testUnescapedSingleCharUnicodeChar() {
        Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("Delimiter", "\\u0001", mockContext);
        assertTrue(result.isValid());
    }

    @Test
    public void testUnescapedSingleCharGoodChar() {
        Validator validator = CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR;
        ValidationContext mockContext = Mockito.mock(ValidationContext.class);
        ValidationResult result = validator.validate("Delimiter", ",", mockContext);
        assertTrue(result.isValid());
    }

}
