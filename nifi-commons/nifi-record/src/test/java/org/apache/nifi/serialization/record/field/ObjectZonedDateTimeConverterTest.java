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
package org.apache.nifi.serialization.record.field;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.regex.Matcher;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectZonedDateTimeConverterTest {
    @ParameterizedTest
    @MethodSource("getPatterns")
    void testTimeZonePattern(String pattern, boolean expected) {
        final Matcher matcher = ObjectZonedDateTimeConverter.TIMEZONE_PATTERN.matcher(pattern);
        if (expected) {
            assertTrue(matcher.find());
        } else {
            assertFalse(matcher.find());
        }
    }

    private static Stream<Arguments> getPatterns() {
        return Stream.of(
                Arguments.of("yyyy-MM-dd'T'HH:mm:ssZ", true),
                Arguments.of("Zyyyy-MM-dd'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'ZHH:mm:ss", true),
                Arguments.of("yyyy-MM-ddZ'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'HH:mm:ss'Z'", false),
                Arguments.of("EEEE, MMM dd, yyyy HH:mm:ss a", false),
                Arguments.of("dd-MMM-yyyy", false),
                Arguments.of("MMMM dd, yyyy: EEEE", false)
        );
    }
}
