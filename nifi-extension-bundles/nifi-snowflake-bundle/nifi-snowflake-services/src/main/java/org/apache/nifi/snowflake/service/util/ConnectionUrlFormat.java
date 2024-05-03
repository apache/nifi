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

package org.apache.nifi.snowflake.service.util;

import org.apache.nifi.components.DescribedValue;

import java.util.Objects;
import java.util.Optional;

public enum ConnectionUrlFormat implements DescribedValue {
    FULL_URL("full-url", "Full URL", "Provide connection URL in a single property") {
        @Override
        public String buildConnectionUrl(final ConnectionUrlFormatParameters parameters) {
            String snowflakeUrl = parameters.getSnowflakeUrl();
            if (!snowflakeUrl.startsWith(SNOWFLAKE_SCHEME)) {
                snowflakeUrl = SNOWFLAKE_URI_PREFIX + snowflakeUrl;
            }

            return snowflakeUrl;
        }
    },
    ACCOUNT_NAME("account-name", "Account Name", "Provide a Snowflake Account Name") {
        @Override
        public String buildConnectionUrl(final ConnectionUrlFormatParameters parameters) {
            final String organizationName = Objects.requireNonNull(parameters.getOrganizationName());
            final String accountName = Objects.requireNonNull(parameters.getAccountName());
            return SNOWFLAKE_URI_PREFIX + organizationName + "-" + accountName + SNOWFLAKE_HOST_SUFFIX;
        }
    },
    ACCOUNT_LOCATOR("account-locator", "Account Locator", "Provide a Snowflake Account Locator") {
        @Override
        public String buildConnectionUrl(final ConnectionUrlFormatParameters parameters) {
            final String accountLocator = Objects.requireNonNull(parameters.getAccountLocator());
            final String cloudRegion = Objects.requireNonNull(parameters.getCloudRegion());
            final String optCloudType = parameters.getCloudType();

            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(SNOWFLAKE_URI_PREFIX).append(accountLocator)
                    .append(".").append(cloudRegion);
            Optional.ofNullable(optCloudType)
                    .ifPresent(cloudType -> stringBuilder.append(".").append(cloudType));
            stringBuilder.append(SNOWFLAKE_HOST_SUFFIX);
            return stringBuilder.toString();
        }
    };

    public static final String SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com";
    public static final String SNOWFLAKE_SCHEME = "jdbc:snowflake";
    public static final String SNOWFLAKE_URI_PREFIX = SNOWFLAKE_SCHEME + "://";

    private final String value;
    private final String displayName;
    private final String description;

    ConnectionUrlFormat(final String value, final String displayName, final String description) {
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

    public abstract String buildConnectionUrl(final ConnectionUrlFormatParameters parameters);

}
