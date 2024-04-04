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

package org.apache.nifi.processors.snowflake.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class SnowflakeProperties {
    private SnowflakeProperties() {
    }

    public static final PropertyDescriptor ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .name("account-locator")
            .displayName("Account Locator")
            .description("Snowflake account locator to use for connection.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor CLOUD_REGION = new PropertyDescriptor.Builder()
            .name("cloud-region")
            .displayName("Cloud Region")
            .description("Snowflake cloud region to use for connection.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor CLOUD_TYPE = new PropertyDescriptor.Builder()
            .name("cloud-type")
            .displayName("Cloud Type")
            .description("Snowflake cloud type to use for connection.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .name("organization-name")
            .displayName("Organization Name")
            .description("Snowflake organization name to use for connection.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("account-name")
            .displayName("Account Name")
            .description("Snowflake account name to use for connection.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("Database")
            .description("The database to use by default. The same as passing 'db=DATABASE_NAME' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Schema")
            .description("The schema to use by default. The same as passing 'schema=SCHEMA' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
}
