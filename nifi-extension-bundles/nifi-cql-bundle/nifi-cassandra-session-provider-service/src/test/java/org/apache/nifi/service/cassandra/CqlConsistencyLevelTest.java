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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.apache.nifi.service.cql.api.constants.CqlConsistencyLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Pins {@link CqlConsistencyLevel}'s values against the DataStax driver's own enum.
 *
 * <p>{@code CqlConsistencyLevel} deliberately declares these names itself rather than deriving them from the
 * driver, so that {@code nifi-cql-services-api} - the backend-agnostic contract every other module compiles
 * against - carries no dependency on a database client library. The cost of that choice is that nothing in the
 * type system keeps the two in step, and the selected value is handed to the driver as a plain string:
 * {@code CassandraCQLExecutionService} sets it as {@code DefaultDriverOption.REQUEST_CONSISTENCY}, which the
 * driver resolves with {@code DefaultConsistencyLevel.valueOf}. A name the driver cannot parse therefore fails
 * at runtime on a configured service rather than at build time - which is what this test exists to prevent.
 *
 * <p>This test lives here, in the module that already depends on the driver, rather than beside the enum:
 * {@code nifi-cql-services-api} is the module the {@code ban-database-client-dependencies} enforcer rule
 * guards, and that rule has no scope exemption, so even a test-scoped driver dependency there would fail the
 * build. {@code ScyllaConsistencyLevelTest} makes the same assertions against ScyllaDB's fork, which ships its
 * own copy of the driver enum.
 */
class CqlConsistencyLevelTest {

    @ParameterizedTest
    @EnumSource(CqlConsistencyLevel.class)
    @DisplayName("Every declared level's value is a name the DataStax driver can parse, since it is passed straight through as a config string")
    void testValueParsesAsDriverConsistencyLevel(final CqlConsistencyLevel level) {
        assertDoesNotThrow(() -> DefaultConsistencyLevel.valueOf(level.getValue()),
                () -> level.name() + " declares the value '" + level.getValue()
                        + "', which the driver does not recognise. Known levels: "
                        + Arrays.stream(DefaultConsistencyLevel.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", ")));
    }

    @ParameterizedTest
    @EnumSource(CqlConsistencyLevel.class)
    @DisplayName("The declared value matches the driver level it resolves to, so the two enums cannot drift apart silently")
    void testValueMatchesTheResolvedDriverLevel(final CqlConsistencyLevel level) {
        assertEquals(level.getValue(), DefaultConsistencyLevel.valueOf(level.getValue()).name());
    }

    // Deliberately one-directional: exposing a subset of the driver's levels is a product decision, so a level
    // the driver gains but this enum does not offer is not a failure. The direction that matters is that
    // everything offered in the UI works.

    @ParameterizedTest
    @EnumSource(CqlConsistencyLevel.class)
    @DisplayName("Every level carries a display name and a description, which are what the Consistency Level dropdown renders")
    void testDescribedValueContractIsPopulated(final CqlConsistencyLevel level) {
        assertFalse(level.getDisplayName() == null || level.getDisplayName().isBlank(),
                () -> level.name() + " has no display name");
        assertFalse(level.getDescription() == null || level.getDescription().isBlank(),
                () -> level.name() + " has no description; the dropdown would show a bare name");
    }
}
