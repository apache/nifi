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
package org.apache.nifi.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.JsonValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestJsonValidator {
    private static final String JSON_PROPERTY = "JSONProperty";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    ValidationContext context;

    private final Validator validator = JsonValidator.INSTANCE;

    @Test
    public void testFlat() throws JsonProcessingException {
        String msg = MAPPER.writeValueAsString(Collections.singletonMap("Name", "Crockford, Douglas"));
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, msg, context);

        assertTrue(validationResult.isValid());
    }

    @Test
    public void testNested() throws JsonProcessingException {
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("Name", "Crockford, Douglas");
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("Mobile", 987654321);
        nested.put("Email", "mrx@xyz.zyx");
        outer.put("ContactInfo", nested);
        String msg = MAPPER.writeValueAsString(outer);
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, msg, context);

        assertTrue(validationResult.isValid());
    }

    @Test
    public void testObjectWithArray() throws JsonProcessingException {
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("name", "Smith, John");
        outer.put("age", 30);
        outer.put("cars", Arrays.asList("Ford", "BMW", "Fiat"));
        String msg = MAPPER.writeValueAsString(outer);
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, msg, context);

        assertTrue(validationResult.isValid());
    }

    @Test
    public void testJSONArray() throws JsonProcessingException {
        String msg = MAPPER.writeValueAsString(Arrays.asList("one", "two", "three"));
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, msg, context);

        assertTrue(validationResult.isValid());
    }

    @Test
    public void testEmpty() {
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, "{}", context);
        assertTrue(validationResult.isValid());
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"Name\" : \"Smith, John\"", "bncjbhjfjhj"})
    public void testInvalidJson(String invalidJson) {
        ValidationResult validationResult = validator.validate(JSON_PROPERTY, invalidJson, context);
        assertFalse(validationResult.isValid());
        assertTrue(validationResult.getExplanation().contains("not a valid JSON representation"));
    }
}
