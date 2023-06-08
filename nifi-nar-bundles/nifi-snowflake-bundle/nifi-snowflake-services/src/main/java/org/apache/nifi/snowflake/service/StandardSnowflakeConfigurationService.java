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

package org.apache.nifi.snowflake.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.SnowflakeConfiguration;
import org.apache.nifi.processors.snowflake.SnowflakeConfigurationService;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormat;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormatParameters;
import org.apache.nifi.snowflake.service.util.SnowflakeConstants;

import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tags({"snowflake"})
@CapabilityDescription("Provides Snowflake configuration properties")
public class StandardSnowflakeConfigurationService extends AbstractControllerService implements SnowflakeConfigurationService {

    public static final PropertyDescriptor ACCOUNT_IDENTIFIER_FORMAT = new PropertyDescriptor.Builder()
            .name("account-identifier-format")
            .displayName("Account Identifier Format")
            .description("The format of the account identifier.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .allowableValues(AccountIdentifierFormat.class)
            .defaultValue(AccountIdentifierFormat.ACCOUNT_NAME.getValue())
            .build();

    public static final PropertyDescriptor ACCOUNT_URL = new PropertyDescriptor.Builder()
            .name("account-url")
            .displayName("Account URL")
            .description("The Snowflake account URL or hostname (in this case https with default port is assumed). " +
                    "Example: [account-locator].[cloud-region].[cloud]" + SnowflakeConstants.SNOWFLAKE_HOST_SUFFIX)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_URL)
            .build();

    public static final PropertyDescriptor ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_LOCATOR)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_REGION)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_TYPE)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ORGANIZATION_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("The Snowflake username.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("private-key-service")
            .displayName("Private Key Service")
            .description("Specifies the Controller Service that will provide the private key. The public key needs to be added to the user account in the Snowflake account beforehand.")
            .identifiesControllerService(PrivateKeyService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor ROLE = new PropertyDescriptor.Builder()
            .name("role")
            .displayName("Role")
            .description("The Snowflake role.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ACCOUNT_IDENTIFIER_FORMAT,
            ACCOUNT_URL,
            ACCOUNT_LOCATOR,
            CLOUD_REGION,
            CLOUD_TYPE,
            ORGANIZATION_NAME,
            ACCOUNT_NAME,
            USERNAME,
            PRIVATE_KEY_SERVICE,
            ROLE
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private volatile SnowflakeConfiguration configuration;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final AccountIdentifierFormat accountIdentifierFormat = AccountIdentifierFormat.forName(context.getProperty(ACCOUNT_IDENTIFIER_FORMAT).getValue());

        final AccountIdentifierFormatParameters accountIdentifierFormatParameters = getAccountIdentifierFormatParameters(context);
        final String accountUrl = accountIdentifierFormat.getAccountUrl(accountIdentifierFormatParameters);
        final String account = accountIdentifierFormat.getAccount(accountIdentifierFormatParameters);

        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();

        final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
        final PrivateKey privateKey = privateKeyService.getPrivateKey();

        final String role = context.getProperty(ROLE).evaluateAttributeExpressions().getValue();

        configuration = new SnowflakeConfiguration(accountUrl, account, username, privateKey, role);
    }

    @Override
    public SnowflakeConfiguration getConfiguration() {
        return configuration;
    }

    private AccountIdentifierFormatParameters getAccountIdentifierFormatParameters(ConfigurationContext context) {
        final String accountUrl = context.getProperty(ACCOUNT_URL)
                .evaluateAttributeExpressions()
                .getValue();
        final String organizationName = context.getProperty(ORGANIZATION_NAME)
                .evaluateAttributeExpressions()
                .getValue();
        final String accountName = context.getProperty(ACCOUNT_NAME)
                .evaluateAttributeExpressions()
                .getValue();
        final String accountLocator = context.getProperty(ACCOUNT_LOCATOR)
                .evaluateAttributeExpressions()
                .getValue();
        final String cloudRegion = context.getProperty(CLOUD_REGION)
                .evaluateAttributeExpressions()
                .getValue();
        final String cloudType = context.getProperty(CLOUD_TYPE)
                .evaluateAttributeExpressions()
                .getValue();

        return new AccountIdentifierFormatParameters(accountUrl,
                organizationName,
                accountName,
                accountLocator,
                cloudRegion,
                cloudType);
    }
}
