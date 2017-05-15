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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "logging"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Emits a log message at the specified log level")
public class LogMessage extends AbstractProcessor {

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log-level")
            .displayName("Log Level")
            .required(true)
            .description("The Log Level to use when logging the message")
            .allowableValues(MessageLogLevel.values())
            .defaultValue(MessageLogLevel.info.toString())
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
            .name("log-prefix")
            .displayName("Log prefix")
            .required(false)
            .description("Log prefix appended to the log lines. " +
                    "It helps to distinguish the output of multiple LogMessage processors.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor LOG_MESSAGE = new PropertyDescriptor.Builder()
            .name("log-message")
            .displayName("Log message")
            .required(false)
            .description("The log message to emit")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

    private static final int CHUNK_SIZE = 50;

    enum MessageLogLevel {

        trace, debug, info, warn, error
    }

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> supportedDescriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(LOG_LEVEL);
        supDescriptors.add(LOG_PREFIX);
        supDescriptors.add(LOG_MESSAGE);
        supportedDescriptors = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final String logLevelValue = context.getProperty(LOG_LEVEL).getValue().toLowerCase();

        final MessageLogLevel logLevel;
        try {
            logLevel = MessageLogLevel.valueOf(logLevelValue);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        final ComponentLog logger = getLogger();
        boolean isLogLevelEnabled = false;
        switch (logLevel) {
            case trace:
                isLogLevelEnabled = logger.isTraceEnabled();
                break;
            case debug:
                isLogLevelEnabled = logger.isDebugEnabled();
                break;
            case info:
                isLogLevelEnabled = logger.isInfoEnabled();
                break;
            case warn:
                isLogLevelEnabled = logger.isWarnEnabled();
                break;
            case error:
                isLogLevelEnabled = logger.isErrorEnabled();
                break;
        }

        if (!isLogLevelEnabled) {
            transferChunk(session);
            return;
        }

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        processFlowFile(logger, logLevel, flowFile, context);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private void processFlowFile(
            final ComponentLog logger,
            final MessageLogLevel logLevel,
            final FlowFile flowFile,
            final ProcessContext context) {

        String logPrefix = context.getProperty(LOG_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        String logMessage = context.getProperty(LOG_MESSAGE).evaluateAttributeExpressions(flowFile).getValue();

        String messageToWrite;
        if (StringUtil.isBlank(logPrefix)) {
            messageToWrite = logMessage;
        } else {
            messageToWrite = String.format("%s%s", logPrefix, logMessage);
        }

        // Uses optional property to specify logging level
        switch (logLevel) {
            case info:
                logger.info(messageToWrite);
                break;
            case debug:
                logger.debug(messageToWrite);
                break;
            case warn:
                logger.warn(messageToWrite);
                break;
            case trace:
                logger.trace(messageToWrite);
                break;
            case error:
                logger.error(messageToWrite);
                break;
            default:
                logger.debug(messageToWrite);
        }
    }

    private void transferChunk(final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(CHUNK_SIZE);
        if (!flowFiles.isEmpty()) {
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }

}
