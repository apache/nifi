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
package org.apache.nifi.rules.handlers;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.rules.Action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"rules", "rules engine", "action", "action handler", "logging", "alerts", "bulletins"})
@CapabilityDescription("Creates alerts as bulletins based on a provided action (usually created by a rules engine).  " +
        "Action objects executed with this Handler should contain \"category\", \"message\", and \"logLevel\" attributes.")
public class AlertHandler extends AbstractActionHandlerService {

    public static final PropertyDescriptor DEFAULT_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("alert-default-log-level")
            .displayName("Default Alert Log Level")
            .required(true)
            .description("The default Log Level that will be used to log an alert message" +
                    " if a log level was not provided in the received action's attributes.")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();

    public static final PropertyDescriptor DEFAULT_CATEGORY = new PropertyDescriptor.Builder()
            .name("alert-default-category")
            .displayName("Default Category")
            .required(true)
            .description("The default category to use when logging alert message "+
                    " if a category was not provided in the received action's attributes.")
            .defaultValue("Rules Triggered Alert")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEFAULT_MESSAGE = new PropertyDescriptor.Builder()
            .name("alert-default-message")
            .displayName("Default Message")
            .required(true)
            .description("The default message to include in alert if an alert message was " +
                    "not provided in the received action's attributes")
            .defaultValue("An alert was triggered by a rules-based action.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor INCLUDE_FACTS = new PropertyDescriptor.Builder()
            .name("alert-include-facts")
            .displayName("Include Fact Data")
            .required(true)
            .description("If true, the alert message will include the facts which triggered this action. Default is false.")
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    private List<PropertyDescriptor> properties;
    private String defaultCategory;
    private String defaultLogLevel;
    private String defaultMessage;
    private Boolean includeFacts;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DEFAULT_LOG_LEVEL);
        properties.add(DEFAULT_CATEGORY);
        properties.add(DEFAULT_MESSAGE);
        properties.add(INCLUDE_FACTS);
        properties.add(ENFORCE_ACTION_TYPE);
        properties.add(ENFORCE_ACTION_TYPE_LEVEL);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        defaultLogLevel = context.getProperty(DEFAULT_LOG_LEVEL).getValue().toUpperCase();
        defaultCategory = context.getProperty(DEFAULT_CATEGORY).getValue();
        defaultMessage = context.getProperty(DEFAULT_MESSAGE).getValue();
        includeFacts = context.getProperty(INCLUDE_FACTS).asBoolean();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void executeAction(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        ComponentLog logger = getLogger();
        if (propertyContext instanceof ReportingContext) {

            ReportingContext context = (ReportingContext) propertyContext;
            Map<String, String> attributes = action.getAttributes();
            if (context.getBulletinRepository() != null) {
                final String category = attributes.getOrDefault("category", defaultCategory);
                final String message = getMessage(attributes.getOrDefault("message", defaultMessage), facts);
                final String level = attributes.getOrDefault("severity", attributes.getOrDefault("logLevel", defaultLogLevel));
                Severity severity;
                try {
                    severity = Severity.valueOf(level.toUpperCase());
                } catch (IllegalArgumentException iae) {
                    severity = Severity.INFO;
                }
                BulletinRepository bulletinRepository = context.getBulletinRepository();
                bulletinRepository.addBulletin(context.createBulletin(category, severity, message));

            } else {
                logger.warn("Bulletin Repository is not available which is unusual. Cannot send a bulletin.");
            }

        } else {
            logger.warn("Reporting context was not provided to create bulletins.");
        }
    }

    protected String getMessage(String alertMessage, Map<String, Object> facts){
        if (includeFacts) {
            final StringBuilder message = new StringBuilder(alertMessage);
            final Set<String> fields = facts.keySet();
            message.append("\n");
            message.append("Alert Facts:\n");
            fields.forEach(field -> {
                message.append("Field: ");
                message.append(field);
                message.append(", Value: ");
                message.append(facts.get(field));
                message.append("\n");
            });
            return message.toString();
        }else{
            return alertMessage;
        }
    }

}
