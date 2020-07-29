package org.apache.nifi.bootstrap.notification;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCustomNotificationService extends AbstractNotificationService {

    private static Logger logger = LoggerFactory.getLogger(TestCustomNotificationService.class);

    public static final PropertyDescriptor CUSTOM_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Custom Hostname")
            .description("The hostname of the Custom Server that is used to send notifications")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();
    public static final PropertyDescriptor CUSTOM_USERNAME = new PropertyDescriptor.Builder()
            .name("Custom Username")
            .description("Username for the account")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor CUSTOM_PASSWORD = new PropertyDescriptor.Builder()
            .name("Custom Password")
            .description("Password for the account")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = new HashMap<>();

    static {
        propertyToContext.put("custom.host", CUSTOM_HOSTNAME);
        propertyToContext.put("custom.user", CUSTOM_USERNAME);
        propertyToContext.put("custom.password", CUSTOM_PASSWORD);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CUSTOM_HOSTNAME);
        properties.add(CUSTOM_USERNAME);
        properties.add(CUSTOM_PASSWORD);
        return properties;
    }

    @Override
    public void notify(NotificationContext context, NotificationType type, String subject, String message) throws NotificationFailedException {
        logger.info(context.getProperty(CUSTOM_HOSTNAME).evaluateAttributeExpressions().getValue());
        logger.info(context.getProperty(CUSTOM_USERNAME).evaluateAttributeExpressions().getValue());
        logger.info(context.getProperty(CUSTOM_PASSWORD).evaluateAttributeExpressions().getValue());
    }
}
