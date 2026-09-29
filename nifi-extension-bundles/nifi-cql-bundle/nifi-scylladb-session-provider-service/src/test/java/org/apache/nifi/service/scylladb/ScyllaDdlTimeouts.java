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

package org.apache.nifi.service.scylladb;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.time.Duration;

/**
 * This class attempts to provide defaults that are forgiving while running a battery of DDL tests on ScyllaDB
 */
final class ScyllaDdlTimeouts {

    /**
     * System property overriding the DDL/schema-agreement timeout ceiling (in seconds) used by the
     * bootstrap sessions in this package, for environments where the default isn't enough (e.g. a
     * contended CI host, or a local machine already running other Docker-heavy test suites).
     */
    static final String DDL_TIMEOUT_PROPERTY = "TEST_SCYLLA_DDL_TIMEOUT_SECONDS";

    private static final int DEFAULT_DDL_TIMEOUT_SECONDS = 30;

    private ScyllaDdlTimeouts() {
    }

    static DriverConfigLoader longSchemaTimeoutConfigLoader() {
        final Duration timeout = Duration.ofSeconds(Integer.getInteger(DDL_TIMEOUT_PROPERTY, DEFAULT_DDL_TIMEOUT_SECONDS));
        return DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, timeout)
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, timeout)
                .build();
    }
}
