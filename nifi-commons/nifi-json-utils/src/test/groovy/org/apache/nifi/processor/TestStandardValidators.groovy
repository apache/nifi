/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.processor

import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.components.Validator
import org.apache.nifi.processor.util.JsonValidator
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue
import static org.mockito.Mockito.mock

import static groovy.json.JsonOutput.*

class TestStandardValidators {
    final String DUMMY_JSON_PROPERTY = "JSONProperty"
    Validator validator
    ValidationContext context

    @Before
    void setup() {
        validator = JsonValidator.INSTANCE
        context = mock(ValidationContext.class)
    }

    @Test
    void testFlat() {
        def msg = prettyPrint(toJson([
            Name: "Crockford, Douglas"
        ]))
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, msg, context)
        assertTrue(validationResult.isValid())
    }

    @Test
    void testNested() {
        def msg = prettyPrint(toJson([
            Name: "Crockford, Douglas",
            ContactInfo: [
                Mobile: 987654321,
                Email: "mrx@xyz.zyx"
            ]
        ]))
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, msg, context)
        assertTrue(validationResult.isValid())
    }

    @Test
    void testObjectWithArray() {
        def msg = prettyPrint(toJson([
            name: "Smith, John",
            age: 30,
            cars: [ "Ford", "BMW", "Fiat" ]
        ]))
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, msg, context)
        assertTrue(validationResult.isValid())
    }

    @Test
    void testJSONArray() {
        def msg = prettyPrint(toJson([
            "one", "two", "three"
        ]))
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, msg, context)
        assertTrue(validationResult.isValid())
    }

    @Test
    void testEmpty() {
        // Empty JSON
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, "{}", context)
        assertTrue(validationResult.isValid())
    }

    @Test
    void testInvalidJson() {
        // Invalid JSON
        ValidationResult validationResult = validator.validate(DUMMY_JSON_PROPERTY, "\"Name\" : \"Smith, John\"", context)
        assertFalse(validationResult.isValid())
        assertTrue(validationResult.getExplanation().contains("not a valid JSON representation"))
        validationResult = validator.validate(DUMMY_JSON_PROPERTY, "bncjbhjfjhj", context)
        assertFalse(validationResult.isValid())
    }
}
