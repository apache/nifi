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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.StringUtil;

import com.google.common.collect.Sets;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"attributes", "logging"})
@InputRequirement(Requirement.INPUT_REQUIRED)
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
            .description("A comma-separated list of Attributes to Log. If not specified, all attributes will be logged unless `Attributes to Log by Regular Expression` is modified." +
                    " There's an AND relationship between the two properties.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_LOG_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-log-regex")
            .displayName("Attributes to Log by Regular Expression")
            .required(false)
            .defaultValue(".*")
            .description("A regular expression indicating the Attributes to Log. If not specified, all attributes will be logged unless `Attributes to Log` is modified." +
                    " There's an AND relationship between the two properties.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_CSV = new PropertyDescriptor.Builder()
            .name("Attributes to Ignore")
            .description("A comma-separated list of Attributes to ignore. If not specified, no attributes will be ignored unless `Attributes to Ignore by Regular Expression` is modified." +
                    " There's an OR relationship between the two properties.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-ignore-regex")
            .displayName("Attributes to Ignore by Regular Expression")
            .required(false)
            .description("A regular expression indicating the Attributes to Ignore. If not specified, no attributes will be ignored unless `Attributes to Ignore` is modified." +
                    " There's an OR relationship between the two properties.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor LOG_PAYLOAD = new PropertyDescriptor.Builder()
            .name("Log Payload")
            .required(true)
            .description("If true, the FlowFile's payload will be logged, in addition to its attributes; otherwise, just the Attributes will be logged.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor LOG_PREFIX = new PropertyDescriptor.Builder()
            .name("Log prefix")
            .required(false)
            .description("Log prefix appended to the log lines. It helps to distinguish the output of multiple LogAttribute processors.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("The name of the CharacterSet to use")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(Charset.defaultCharset().name())
            .required(true)
            .build();

    public static final String FIFTY_DASHES = "--------------------------------------------------";

    public static enum DebugLevels {
        trace, debug, info, warn, error
    }

    public static final long ONE_MB = 1024 * 1024;
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> supportedDescriptors;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this relationship")
            .build();

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
        supDescriptors.add(ATTRIBUTES_TO_LOG_REGEX);
        supDescriptors.add(ATTRIBUTES_TO_IGNORE_CSV);
        supDescriptors.add(ATTRIBUTES_TO_IGNORE_REGEX);
        supDescriptors.add(LOG_PREFIX);
        supDescriptors.add(CHARSET);
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

    protected String processFlowFile(final ComponentLog logger, final DebugLevels logLevel, final FlowFile flowFile, final ProcessSession session, final ProcessContext context) {
        final Set<String> attributeKeys = getAttributesToLog(flowFile.getAttributes().keySet(), context);
        final ComponentLog LOG = getLogger();
        final String dashedLine;

        String logPrefix = context.getProperty(LOG_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        if (StringUtil.isBlank(logPrefix)) {
            dashedLine = StringUtils.repeat('-', 50);
        } else {
            // abbreviate long lines
            logPrefix = StringUtils.abbreviate(logPrefix, 40);
            // center the logPrefix and pad with dashes
            logPrefix = StringUtils.center(logPrefix, 40, '-');
            // five dashes on the left and right side, plus the dashed logPrefix
            dashedLine = StringUtils.repeat('-', 5) + logPrefix + StringUtils.repeat('-', 5);
        }

        // Pretty print metadata
        final StringBuilder message = new StringBuilder();
        message.append("logging for flow file ").append(flowFile);
        message.append("\n");
        message.append(dashedLine);
        message.append("\nStandard FlowFile Attributes");
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
        message.append("\nFlowFile Attribute Map Content");
        for (final String key : attributeKeys) {
            message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
        }
        message.append("\n");
        message.append(dashedLine);

        // The user can request to log the payload
        final boolean logPayload = context.getProperty(LOG_PAYLOAD).asBoolean();
        if (logPayload) {
            message.append("\n");
            if (flowFile.getSize() < ONE_MB) {
                final FlowFilePayloadCallback callback = new FlowFilePayloadCallback(charset);
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

        // collect properties
        final String attrsToLogValue = context.getProperty(ATTRIBUTES_TO_LOG_CSV).getValue();
        final String attrsToRemoveValue = context.getProperty(ATTRIBUTES_TO_IGNORE_CSV).getValue();
        final Set<String> attrsToLog = StringUtils.isBlank(attrsToLogValue) ? Sets.newHashSet(flowFileAttrKeys) : Sets.newHashSet(attrsToLogValue.split("\\s*,\\s*"));
        final Set<String> attrsToRemove = StringUtils.isBlank(attrsToRemoveValue) ? Sets.newHashSet() : Sets.newHashSet(attrsToRemoveValue.split("\\s*,\\s*"));
        final Pattern attrsToLogRegex = Pattern.compile(context.getProperty(ATTRIBUTES_TO_LOG_REGEX).getValue());
        final String attrsToRemoveRegexValue = context.getProperty(ATTRIBUTES_TO_IGNORE_REGEX).getValue();
        final Pattern attrsToRemoveRegex = attrsToRemoveRegexValue == null ? null : Pattern.compile(context.getProperty(ATTRIBUTES_TO_IGNORE_REGEX).getValue());
        return flowFileAttrKeys.stream()
                .filter(candidate -> {
                    // we'll consider logging an attribute if either no explicit attributes to log were configured,
                    // if this property was configured to be logged, or if the regular expression of properties to log matches
                    if ((attrsToLog.isEmpty() || attrsToLog.contains(candidate)) && attrsToLogRegex.matcher(candidate).matches()) {
                        // log properties we've _not_ configured either explicitly or by regular expression to be ignored.
                        if ((attrsToRemove.isEmpty() || !attrsToRemove.contains(candidate)) && (attrsToRemoveRegex == null || !attrsToRemoveRegex.matcher(candidate).matches())) {
                            return true;
                        }
                    }
                    return false;
                }).collect(Collectors.toCollection(TreeSet::new));
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

        final ComponentLog LOG = getLogger();
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
        private Charset charset;

        public FlowFilePayloadCallback(Charset charset) {
            this.charset = charset;
        }

        @Override
        public void process(final InputStream in) throws IOException {
            contents = IOUtils.toString(in, charset);
        }

        public String getContents() {
            return contents;
        }
    }

}
