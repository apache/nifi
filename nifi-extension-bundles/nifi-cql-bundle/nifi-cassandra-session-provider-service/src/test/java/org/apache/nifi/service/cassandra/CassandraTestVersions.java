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

import java.util.stream.Stream;

/**
 * Supplies the Cassandra major versions that the {@code @ParameterizedClass} integration tests in this
 * package run against. Cassandra 3.11 and 4.1 containers are slow to start and are no longer receiving
 * upstream support, so they're only exercised when the {@code TEST_CASSANDRA_OLDER_VERSIONS} system
 * property is set to {@code true}; otherwise only the current major version (5.0) runs.
 */
final class CassandraTestVersions {

    private static final String RUN_OLDER_VERSIONS_PROPERTY = "TEST_CASSANDRA_OLDER_VERSIONS";

    /**
     * The current major version, and the only one exercised unless {@code TEST_CASSANDRA_OLDER_VERSIONS} is
     * set. Suites whose behaviour does not vary by release pin to this rather than declaring a version of
     * their own, so that they share {@code SharedCassandraCluster}'s container for it instead of silently
     * starting a second one the day this is bumped.
     */
    static final String CURRENT_VERSION = "5.0";

    private CassandraTestVersions() {
    }

    static Stream<String> allVersions() {
        return Boolean.getBoolean(RUN_OLDER_VERSIONS_PROPERTY)
                ? Stream.of("3.11", "4.1", CURRENT_VERSION)
                : Stream.of(CURRENT_VERSION);
    }
}
