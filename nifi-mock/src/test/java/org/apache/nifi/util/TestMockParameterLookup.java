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
package org.apache.nifi.util;

import org.apache.nifi.parameter.Parameter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMockParameterLookup {

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldRetrieveExistingParameter(final String key, final String value) {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(key, value);

        final MockParameterLookup lookup = new MockParameterLookup(parameters);

        final Optional<Parameter> parameter = lookup.getParameter(key);
        assertTrue(parameter.isPresent());
        assertEquals(key, parameter.get().getDescriptor().getName());
        assertEquals(value, parameter.get().getValue());
        assertFalse(parameter.get().getDescriptor().isSensitive());
    }

    private static Stream<Arguments> getParameters() {
        return Stream.of(
            Arguments.of("test.param", "test-value"),
            Arguments.of("empty-param", "")
        );
    }

    @Test
    void shouldNotFailOnEmptyOrNullParametersMap() {
        final MockParameterLookup emptyLookup = new MockParameterLookup(new HashMap<>());
        assertTrue(emptyLookup.isEmpty());

        final MockParameterLookup nullLookup = new MockParameterLookup(null);
        assertTrue(nullLookup.isEmpty());
    }

    @Test
    void shouldNotRetrieveNonExistingParameter() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("existing.param", "value");

        final MockParameterLookup lookup = new MockParameterLookup(parameters);

        final Optional<Parameter> parameter = lookup.getParameter("non.existing.param");
        assertFalse(parameter.isPresent());
    }

    @Test
    void shouldNotRetrieveParameterWithNullValue() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("null_param", null);

        final MockParameterLookup lookup = new MockParameterLookup(parameters);

        final Optional<Parameter> parameter = lookup.getParameter("null_param");
        assertFalse(parameter.isPresent());
    }
}
