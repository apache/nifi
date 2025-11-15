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
package org.apache.nifi.dbcp.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Provides contextual information to {@link DatabasePasswordProvider} implementations when a password is requested.
 */
public class DatabasePasswordRequestContext {
    private final String jdbcUrl;
    private final String databaseUser;
    private final String driverClassName;
    private final Map<String, String> connectionProperties;

    private DatabasePasswordRequestContext(final Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.databaseUser = builder.databaseUser;
        this.driverClassName = builder.driverClassName;
        this.connectionProperties = builder.connectionProperties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.connectionProperties);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public Map<String, String> getConnectionProperties() {
        return connectionProperties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jdbcUrl;
        private String databaseUser;
        private String driverClassName;
        private Map<String, String> connectionProperties;

        public Builder jdbcUrl(final String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder databaseUser(final String databaseUser) {
            this.databaseUser = databaseUser;
            return this;
        }

        public Builder driverClassName(final String driverClassName) {
            this.driverClassName = driverClassName;
            return this;
        }

        public Builder connectionProperties(final Map<String, String> connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public DatabasePasswordRequestContext build() {
            Objects.requireNonNull(jdbcUrl, "JDBC URL required");
            Objects.requireNonNull(driverClassName, "Driver Class Name required");
            return new DatabasePasswordRequestContext(this);
        }
    }
}
