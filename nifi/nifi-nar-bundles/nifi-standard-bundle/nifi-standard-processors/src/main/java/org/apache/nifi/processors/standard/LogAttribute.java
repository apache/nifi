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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "logging"})
public class LogAttribute extends AbstractProcessor {

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .required(true)
            .description("The Log Level to use when logging the Attributes")
            .allowableValues(DebugLevels.values())
            .defaultValue("info")
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_LOG_CSV = new PropertyDescriptor.Builder()
            .name("Attributes to Log")
            .required(false)
            .description("A comma-separated list of Attributes to Log. If not specified, all attributes will be logged.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_CSV = new PropertyDescriptor.Builder()
            .name("Attributes to Ignore")
            .description("A comma-separated list of Attributes to ignore. If not specified, no attributes will be ignored.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor LOG_PAYLOAD = new PropertyDescriptor.Builder()
            .name("Log Payload")
            .required(true)
            .description("If true, the FlowFile's payload will be logged, in addition to its attributes; otherwise, just the Attributes will be logged.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final String FIFTY_DASHES = "--------------------------------------------------";

    public static enum DebugLevels {

        trace, debug, info, warn, error
    }

    public static final long ONE_MB = 1024 * 1024;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> supportedDescriptors;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to this relationship").build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(LOG_LEVEL);
        supDescriptors.add(LOG_PAYLOAD);
        supDescriptors.add(ATTRIBUTES_TO_LOG_CSV);
        supDescriptors.add(ATTRIBUTES_TO_IGNORE_CSV);
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

    protected String processFlowFile(final ProcessorLog logger, final DebugLevels logLevel, final FlowFile flowFile, final ProcessSession session, final ProcessContext context) {
        final Set<String> attributeKeys = getAttributesToLog(flowFile.getAttributes().keySet(), context);
        final ProcessorLog LOG = getLogger();

        // Pretty print metadata
        final StringBuilder message = new StringBuilder();
        message.append("logging for flow file ").append(flowFile);
        message.append("\n");
        message.append(FIFTY_DASHES);
        message.append("\nStandard FlowFile Attributes");
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
        message.append("\nFlowFile Attribute Map Content");
        for (final String key : attributeKeys) {
            message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
        }
        message.append("\n");
        message.append(FIFTY_DASHES);

        // The user can request to log the payload
        final boolean logPayload = context.getProperty(LOG_PAYLOAD).asBoolean();
        if (logPayload) {
            message.append("\n");
            if (flowFile.getSize() < ONE_MB) {
                final FlowFilePayloadCallback callback = new FlowFilePayloadCallback();
                session.read(flowFile, callback);
                message.append(callback.getContents());
            } else {
                message.append("\n Not including payload since it is larger than one mb.");
            }
        }
        final String outputMessage = message.toString().trim();
        // Uses optional property to specify logging level
        switch (logLevel) {
            case info:
                LOG.info(outputMessage);
                break;
            case debug:
                LOG.debug(outputMessage);
                break;
            case warn:
                LOG.warn(outputMessage);
                break;
            case trace:
                LOG.trace(outputMessage);
                break;
            case error:
                LOG.error(outputMessage);
                break;
            default:
                LOG.debug(outputMessage);
        }

        return outputMessage;

    }

    private Set<String> getAttributesToLog(final Set<String> flowFileAttrKeys, final ProcessContext context) {
        final Set<String> result = new TreeSet<>();

        final String attrsToLogValue = context.getProperty(ATTRIBUTES_TO_LOG_CSV).getValue();
        if (StringUtils.isBlank(attrsToLogValue)) {
            result.addAll(flowFileAttrKeys);
        } else {
            result.addAll(Arrays.asList(attrsToLogValue.split("\\s*,\\s*")));
        }

        final String attrsToRemoveValue = context.getProperty(ATTRIBUTES_TO_IGNORE_CSV).getValue();
        if (StringUtils.isNotBlank(attrsToRemoveValue)) {
            result.removeAll(Arrays.asList(attrsToRemoveValue.split("\\s*,\\s*")));
        }

        return result;
    }

    private void transferChunk(final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(50);
        if (!flowFiles.isEmpty()) {
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String logLevelValue = context.getProperty(LOG_LEVEL).getValue().toLowerCase();

        final DebugLevels logLevel;
        try {
            logLevel = DebugLevels.valueOf(logLevelValue);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        final ProcessorLog LOG = getLogger();
        boolean isLogLevelEnabled = false;
        switch (logLevel) {
            case trace:
                isLogLevelEnabled = LOG.isTraceEnabled();
                break;
            case debug:
                isLogLevelEnabled = LOG.isDebugEnabled();
                break;
            case info:
                isLogLevelEnabled = LOG.isInfoEnabled();
                break;
            case warn:
                isLogLevelEnabled = LOG.isWarnEnabled();
                break;
            case error:
                isLogLevelEnabled = LOG.isErrorEnabled();
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

        processFlowFile(LOG, logLevel, flowFile, session, context);
        session.transfer(flowFile, REL_SUCCESS);
    }

    protected static class FlowFilePayloadCallback implements InputStreamCallback {

        private String contents = "";

        @Override
        public void process(final InputStream in) throws IOException {
            contents = IOUtils.toString(in);
        }

        public String getContents() {
            return contents;
        }
    }

}
