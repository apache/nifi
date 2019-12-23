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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"rules", "rules engine", "action", "action handler", "logging"})
@CapabilityDescription("Logs messages and fact information based on a provided action (usually created by a rules engine).  " +
        " Action objects executed with this Handler should contain \"logLevel\" and \"message\" attributes.")
public class LogHandler extends AbstractActionHandlerService {

    public static final PropertyDescriptor DEFAULT_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("logger-default-log-level")
            .displayName("Default Log Level")
            .required(true)
            .description("If a log level is not provided as an attribute within an Action, the default log level will be used.")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();

    public static final PropertyDescriptor DEFAULT_LOG_MESSAGE = new PropertyDescriptor.Builder()
            .name("logger-default-log-message")
            .displayName("Default Log Message")
            .required(true)
            .description("If a log message is not provided as an attribute within an Action, the default log message will be used.")
            .defaultValue("Rules Action Triggered Log.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor LOG_FACTS = new PropertyDescriptor.Builder()
            .name("log-facts")
            .displayName("Log Facts")
            .required(true)
            .description("If true, the log message will include the facts which triggered this log action.")
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    private static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
            .name("log-prefix")
            .displayName("Log Prefix")
            .required(false)
            .description("Log prefix appended to the log lines. It helps to distinguish the output of multiple LogAttribute processors.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private List<PropertyDescriptor> properties;
    private String logPrefix;
    private Boolean logFacts;
    private String defaultLogLevel;
    private String defaultLogMessage;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LOG_PREFIX);
        properties.add(LOG_FACTS);
        properties.add(DEFAULT_LOG_LEVEL);
        properties.add(DEFAULT_LOG_MESSAGE);
        properties.add(ENFORCE_ACTION_TYPE);
        properties.add(ENFORCE_ACTION_TYPE_LEVEL);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        logPrefix = context.getProperty(LOG_PREFIX).evaluateAttributeExpressions().getValue();
        logFacts = context.getProperty(LOG_FACTS).asBoolean();
        defaultLogLevel = context.getProperty(DEFAULT_LOG_LEVEL).getValue().toUpperCase();
        defaultLogMessage = context.getProperty(DEFAULT_LOG_MESSAGE).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void executeAction(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        executeAction(action, facts);
    }

    @Override
    protected void executeAction(Action action, Map<String, Object> facts) {
        ComponentLog logger = getLogger();
        Map<String, String> attributes = action.getAttributes();
        final String logLevel = attributes.get("logLevel");
        final LogLevel level = getLogLevel(logLevel, LogLevel.valueOf(defaultLogLevel));
        final String eventMessage = StringUtils.isNotEmpty(attributes.get("message")) ? attributes.get("message") : defaultLogMessage;
        final String factsMessage = createFactsLogMessage(facts, eventMessage);
        logger.log(level, factsMessage);
    }

    private LogLevel getLogLevel(String logLevel, LogLevel defaultLevel) {
        LogLevel level;
        if (StringUtils.isNotEmpty(logLevel)) {
            try {
                level = LogLevel.valueOf(logLevel.toUpperCase());
            } catch (IllegalArgumentException iea) {
                level = defaultLevel;
            }
        } else {
            level = defaultLevel;
        }
        return level;
    }

    protected String createFactsLogMessage(Map<String, Object> facts, String eventMessage) {

        final Set<String> fields = facts.keySet();
        final StringBuilder message = new StringBuilder();
        String dashedLine;

        if (StringUtils.isBlank(logPrefix)) {
            dashedLine = StringUtils.repeat('-', 50);
        } else {
            // abbreviate long lines
            logPrefix = StringUtils.abbreviate(logPrefix, 40);
            // center the logPrefix and pad with dashes
            logPrefix = StringUtils.center(logPrefix, 40, '-');
            // five dashes on the left and right side, plus the dashed logPrefix
            dashedLine = StringUtils.repeat('-', 5) + logPrefix + StringUtils.repeat('-', 5);
        }

        message.append("\n");
        message.append(dashedLine);
        message.append("\n");
        message.append("Log Message: ");
        message.append(eventMessage);
        message.append("\n");

        if (logFacts) {
            message.append("Log Facts:\n");
            fields.forEach(field -> {
                message.append("Field: ");
                message.append(field);
                message.append(", Value: ");
                message.append(facts.get(field));
                message.append("\n");
            });
        }

        return message.toString().trim();

    }

}
