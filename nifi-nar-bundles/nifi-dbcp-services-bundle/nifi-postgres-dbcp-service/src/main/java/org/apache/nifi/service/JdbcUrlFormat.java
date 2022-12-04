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
package org.apache.nifi.service;

import org.apache.nifi.components.DescribedValue;

import java.util.stream.Stream;

public enum JdbcUrlFormat implements DescribedValue {

    FULL_URL("full-url", "Full URL", "Provide JDBC URL in a single property"),
    PARAMETERS("url-parameters", "URL Parameters", "Provide URL by parameters");

    public static final String POSTGRESQL_SCHEME = "jdbc:postgresql";
    public static final String POSTGRESQL_BASIC_URI = POSTGRESQL_SCHEME + ":/";
    public static final String POSTGRESQL_DB_URI_TEMPLATE = POSTGRESQL_SCHEME + ":%s";
    public static final String POSTGRESQL_HOST_DB_URI_TEMPLATE = POSTGRESQL_SCHEME + "://%s/%s";
    public static final String POSTGRESQL_HOST_PORT_DB_URI_TEMPLATE = POSTGRESQL_SCHEME + "://%s:%s/%s";
    public static final String POSTGRESQL_HOST_PORT_URI_TEMPLATE = POSTGRESQL_SCHEME + "://%s:%s";

    private final String value;
    private final String displayName;
    private final String description;

    JdbcUrlFormat(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static JdbcUrlFormat forName(String urlFormat) {
        return Stream.of(values()).filter(provider -> provider.getValue().equalsIgnoreCase(urlFormat))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Invalid ConnectionUrlFormat: " + urlFormat));
    }
}
