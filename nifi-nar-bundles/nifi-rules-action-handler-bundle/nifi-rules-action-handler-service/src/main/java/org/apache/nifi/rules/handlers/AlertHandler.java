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

@Tags({"rules", "rules engine", "action", "action handler", "logging", "alerts", "bulletins"})
@CapabilityDescription("Creates alerts as bulletins based on a provided action (usually created by a rules engine) ")
public class AlertHandler extends AbstractActionHandlerService {

    public static final PropertyDescriptor DEFAULT_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .required(true)
            .description("The Log Level to use when logging the Attributes")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();

    public static final PropertyDescriptor DEFAULT_CATEGORY = new PropertyDescriptor.Builder()
            .name("Alert Category")
            .required(true)
            .description("The Log Level to use when logging the Attributes")
            .defaultValue("Rules Triggered Alert")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEFAULT_MESSAGE = new PropertyDescriptor.Builder()
            .name("Alert Message")
            .required(true)
            .description("The default message to include in alert")
            .defaultValue("An alert was triggered by a rules based action.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;
    private String defaultCategory;
    private String defaultLogLevel;
    private String defaultMessage;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DEFAULT_LOG_LEVEL);
        properties.add(DEFAULT_CATEGORY);
        properties.add(DEFAULT_MESSAGE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        defaultLogLevel = context.getProperty(DEFAULT_LOG_LEVEL).getValue().toUpperCase();
        defaultCategory = context.getProperty(DEFAULT_CATEGORY).getValue();
        defaultMessage = context.getProperty(DEFAULT_MESSAGE).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void execute(Action action, Map<String, Object> facts) {
        throw new UnsupportedOperationException("This method is not supported.  The AlertHandler requires a Reporting Context");
    }

    @Override
    public void execute(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        ComponentLog logger = getLogger();

        if (propertyContext instanceof ReportingContext) {

            ReportingContext context = (ReportingContext) propertyContext;
            Map<String, String> attributes = action.getAttributes();
            if (context.getBulletinRepository() != null) {
                final String category = attributes.getOrDefault("category", defaultCategory);
                final String message = attributes.getOrDefault("message", defaultMessage);
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

}
