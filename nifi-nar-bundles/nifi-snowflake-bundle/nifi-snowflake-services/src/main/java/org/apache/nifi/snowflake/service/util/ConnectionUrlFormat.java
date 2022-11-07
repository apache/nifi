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

import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_ACCOUNT_LOCATOR;
import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_ACCOUNT_NAME;
import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_CLOUD_REGION;
import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_CLOUD_TYPE;
import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_ORGANIZATION_NAME;
import static org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool.SNOWFLAKE_URL;

import java.util.stream.Stream;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.controller.ConfigurationContext;

public enum ConnectionUrlFormat implements DescribedValue {
    FULL_URL("full-url", "Full URL", "Provide connection URL in a single property") {
        @Override
        public String buildConnectionUrl(final ConfigurationContext context) {
            String snowflakeUrl = context.getProperty(SNOWFLAKE_URL).evaluateAttributeExpressions().getValue();
            if (!snowflakeUrl.startsWith(SNOWFLAKE_SCHEME)) {
                snowflakeUrl = SNOWFLAKE_URI_PREFIX + snowflakeUrl;
            }

            return snowflakeUrl;
        }
    },
    ACCOUNT_NAME("account-name", "Account Name", "Provide a Snowflake Account Name") {
        @Override
        public String buildConnectionUrl(ConfigurationContext context) {
            final String organizationName = context.getProperty(SNOWFLAKE_ORGANIZATION_NAME)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String accountName = context.getProperty(SNOWFLAKE_ACCOUNT_NAME)
                    .evaluateAttributeExpressions()
                    .getValue();

            return SNOWFLAKE_URI_PREFIX + organizationName + "-" + accountName + ".snowflakecomputing.com";
        }
    },
    ACCOUNT_LOCATOR("account-locator", "Account Locator", "Provide a Snowflake Account Locator") {
        @Override
        public String buildConnectionUrl(final ConfigurationContext context) {
            final String accountLocator = context.getProperty(SNOWFLAKE_ACCOUNT_LOCATOR)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String cloudRegion = context.getProperty(SNOWFLAKE_CLOUD_REGION)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String cloudType = context.getProperty(SNOWFLAKE_CLOUD_TYPE)
                    .evaluateAttributeExpressions()
                    .getValue();
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(SNOWFLAKE_URI_PREFIX).append(accountLocator)
                    .append(".").append(cloudRegion);
            if (cloudType != null) {
                stringBuilder.append(".").append(cloudType);
            }
            stringBuilder.append(".snowflakecomputing.com");
            return stringBuilder.toString();
        }
    };

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

    public abstract String buildConnectionUrl(final ConfigurationContext context);

    public static ConnectionUrlFormat forName(String provideMethod) {
        return Stream.of(values()).filter(provider -> provider.getValue().equalsIgnoreCase(provideMethod))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Invalid ConnectionUrlFormat: " + provideMethod));
    }
}
