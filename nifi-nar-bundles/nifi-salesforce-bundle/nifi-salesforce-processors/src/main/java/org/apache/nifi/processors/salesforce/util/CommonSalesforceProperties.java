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
package org.apache.nifi.processors.salesforce.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;

public final class CommonSalesforceProperties {

    public static final PropertyDescriptor SALESFORCE_INSTANCE_URL = new PropertyDescriptor.Builder()
            .name("salesforce-url")
            .displayName("Salesforce Instance URL")
            .description("The URL of the Salesforce instance including the domain without additional path information, such as https://MyDomainName.my.salesforce.com")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("salesforce-api-version")
            .displayName("API Version")
            .description(
                    "The version number of the Salesforce REST API appended to the URL after the services/data path. See Salesforce documentation for supported versions")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("54.0")
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("Maximum time allowed for reading a response from the Salesforce REST API")
            .required(true)
            .defaultValue("15 s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-provider")
            .displayName("OAuth2 Access Token Provider")
            .description("Service providing OAuth2 Access Tokens for authenticating using the HTTP Authorization Header")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .build();

    private CommonSalesforceProperties() {
    }
}
