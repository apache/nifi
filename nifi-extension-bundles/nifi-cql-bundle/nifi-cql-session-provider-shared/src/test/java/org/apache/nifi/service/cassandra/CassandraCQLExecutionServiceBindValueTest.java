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
package org.apache.nifi.service.cassandra;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Covers the conversion applied to a query parameter bound to a {@code timestamp} column, which is the one
 * scalar type with no {@code Flexible*Codec} registered to coerce a value supplied as text.
 *
 * <p>The accepted forms are tabulated rather than written as a method each, so the table reads as the
 * specification of what {@code toInstant} takes.
 */
public class CassandraCQLExecutionServiceBindValueTest {

    private static final Instant EXPECTED = Instant.parse("2026-08-07T14:30:00Z");

    static Stream<Arguments> acceptedForms() {
        return Stream.of(
                arguments("an Instant, passed through", EXPECTED),
                arguments("ISO-8601 text", "2026-08-07T14:30:00Z"),
                arguments("ISO-8601 text with surrounding whitespace", "  2026-08-07T14:30:00Z  "),
                arguments("epoch millis as text", String.valueOf(EXPECTED.toEpochMilli())),
                arguments("epoch millis as a Number", EXPECTED.toEpochMilli()),
                arguments("a java.util.Date", new Date(EXPECTED.toEpochMilli())),
                arguments("a java.sql.Timestamp", Timestamp.from(EXPECTED)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("acceptedForms")
    @DisplayName("Every accepted form of a timestamp parameter converts to the same Instant")
    void testToInstantAcceptedForms(final String description, final Object value) {
        assertEquals(EXPECTED, CassandraCQLExecutionService.toInstant(value));
    }

    @Test
    @DisplayName("A null parameter stays null rather than becoming the epoch")
    void testToInstantIsNullForNull() {
        assertNull(CassandraCQLExecutionService.toInstant(null));
    }

    @Test
    @DisplayName("Text that is neither ISO-8601 nor epoch millis is rejected with the offending value in the message")
    void testToInstantRejectsUnparseableTextWithAttributableMessage() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CassandraCQLExecutionService.toInstant("not a timestamp"));

        assertTrue(e.getMessage().contains("not a timestamp"), e.getMessage());
    }
}
