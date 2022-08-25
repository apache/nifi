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

import java.util.stream.Stream;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.snowflake.service.StandardSnowflakeIngestManagerProviderService;

public enum AccountIdentifierFormat implements DescribedValue {
    FULL_URL("full-url", "Full URL", "Provide an account identifier in a single property") {
        @Override
        public String getAccount(ConfigurationContext context) {
            final String[] hostParts = buildHost(context).split("\\.");
            if (hostParts.length == 0) {
                throw new IllegalArgumentException("Invalid Snowflake host url");
            }
            return hostParts[0];
        }

        @Override
        public String buildHost(final ConfigurationContext context) {
            return context.getProperty(StandardSnowflakeIngestManagerProviderService.HOST_URL)
                    .evaluateAttributeExpressions()
                    .getValue();
        }
    },
    ACCOUNT_NAME("account-name", "Account Name", "Provide a Snowflake Account Name") {
        @Override
        public String getAccount(ConfigurationContext context) {
            final String organizationName = context.getProperty(StandardSnowflakeIngestManagerProviderService.ORGANIZATION_NAME)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String accountName = context.getProperty(StandardSnowflakeIngestManagerProviderService.ACCOUNT_NAME)
                    .evaluateAttributeExpressions()
                    .getValue();
            return organizationName + "-" + accountName;
        }

        @Override
        public String buildHost(final ConfigurationContext context) {
            return getAccount(context) + ".snowflakecomputing.com";
        }
    },
    ACCOUNT_LOCATOR("account-locator", "Account Locator", "Provide a Snowflake Account Locator") {
        @Override
        public String getAccount(ConfigurationContext context) {
            return context.getProperty(StandardSnowflakeIngestManagerProviderService.ACCOUNT_LOCATOR)
                    .evaluateAttributeExpressions()
                    .getValue();
        }

        @Override
        public String buildHost(final ConfigurationContext context) {
            final String accountLocator = context.getProperty(StandardSnowflakeIngestManagerProviderService.ACCOUNT_LOCATOR)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String cloudRegion = context.getProperty(StandardSnowflakeIngestManagerProviderService.CLOUD_REGION)
                    .evaluateAttributeExpressions()
                    .getValue();
            final String cloudType = context.getProperty(StandardSnowflakeIngestManagerProviderService.CLOUD_TYPE)
                    .evaluateAttributeExpressions()
                    .getValue();
            final StringBuilder hostBuilder = new StringBuilder();
            hostBuilder.append(accountLocator)
                    .append(".").append(cloudRegion);
            if (cloudType != null) {
                hostBuilder.append(".").append(cloudType);
            }
            hostBuilder.append(".snowflakecomputing.com");
            return hostBuilder.toString();
        }
    };

    private final String value;
    private final String displayName;
    private final String description;

    AccountIdentifierFormat(final String value, final String displayName, final String description) {
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

    public abstract String getAccount(final ConfigurationContext context);
    public abstract String buildHost(final ConfigurationContext context);

    public static AccountIdentifierFormat forName(String provideMethod) {
        return Stream.of(values()).filter(provider -> provider.getValue().equalsIgnoreCase(provideMethod))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Invalid AccountIdentifierFormat: " + provideMethod));
    }
}
