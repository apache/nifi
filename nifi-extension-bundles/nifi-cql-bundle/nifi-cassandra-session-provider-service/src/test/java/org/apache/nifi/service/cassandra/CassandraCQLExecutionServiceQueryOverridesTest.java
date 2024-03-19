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

import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Covers how a per-query override falls back to the connection service's configured default.
 *
 * <p>Both resolvers have the same three states - no overrides object at all, an overrides object that does not
 * set this particular field, and one that does - and the first two must behave identically. Tabulating them
 * is what makes that equivalence visible.
 */
public class CassandraCQLExecutionServiceQueryOverridesTest {

    private static final int CONFIGURED_FETCH_SIZE = 100;

    static Stream<Arguments> fetchSizeCases() {
        return Stream.of(
                arguments("no overrides supplied", null, CONFIGURED_FETCH_SIZE),
                arguments("overrides supplied, fetch size not set", new QueryOverrides(null, null), CONFIGURED_FETCH_SIZE),
                arguments("fetch size overridden", new QueryOverrides(50, null), 50));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fetchSizeCases")
    @DisplayName("Fetch size falls back to the configured default unless the query overrides it")
    void testResolveFetchSize(final String description, final QueryOverrides overrides, final int expected) {
        assertEquals(expected, CassandraCQLExecutionService.resolveFetchSize(overrides, CONFIGURED_FETCH_SIZE));
    }

    static Stream<Arguments> timeoutCases() {
        return Stream.of(
                arguments("no overrides supplied", null, null),
                arguments("overrides supplied, timeout not set", new QueryOverrides(null, null), null),
                arguments("timeout overridden", new QueryOverrides(null, Duration.ofSeconds(5)), Duration.ofSeconds(5)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timeoutCases")
    @DisplayName("Timeout resolves to null unless the query overrides it, leaving the driver's own default in force")
    void testResolveTimeoutOverride(final String description, final QueryOverrides overrides, final Duration expected) {
        assertEquals(expected, CassandraCQLExecutionService.resolveTimeoutOverride(overrides));
    }
}
