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

package org.apache.nifi.minifi.commons.utils;

import static org.apache.nifi.minifi.commons.utils.PropertyUtil.resolvePropertyValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PropertyUtilTest {

    private static final String PROPERTY_KEY_WITH_DOTS = "property.foo.bar";
    private static final String PROPERTY_KEY_WITH_HYPHENS = "property-foo-bar";
    private static final String PROPERTY_KEY_WITH_DOTS_AND_HYPHENS = "property.foo-bar";
    private static final String PROPERTY_KEY_WITH_UNDERSCORE = "property_foo_bar";
    private static final String VALUE = "value";

    @MethodSource("arguments")
    @ParameterizedTest(name = "{index} => requesting {0} from {1} should return {2}")
    void test(String propertyKey, Map<String, String> propertiesMap, String expectedValue) {
        assertEquals(Optional.ofNullable(expectedValue), resolvePropertyValue(propertyKey, propertiesMap));
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
            Arguments.of(PROPERTY_KEY_WITH_DOTS, Map.of(), null),
            Arguments.of(PROPERTY_KEY_WITH_DOTS, Map.of(PROPERTY_KEY_WITH_DOTS, VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_DOTS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE, VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_HYPHENS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE, VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_DOTS_AND_HYPHENS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE, VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_DOTS, Map.of(PROPERTY_KEY_WITH_DOTS.toUpperCase(), VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_DOTS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE.toUpperCase(), VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_HYPHENS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE.toUpperCase(), VALUE), VALUE),
            Arguments.of(PROPERTY_KEY_WITH_DOTS_AND_HYPHENS, Map.of(PROPERTY_KEY_WITH_UNDERSCORE.toUpperCase(), VALUE), VALUE)
        );
    }
}