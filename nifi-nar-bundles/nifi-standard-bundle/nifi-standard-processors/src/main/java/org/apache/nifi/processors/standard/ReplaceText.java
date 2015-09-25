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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.NLKBufferedReader;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Text", "Regular Expression", "Update", "Change", "Replace", "Modify", "Regex"})
@CapabilityDescription("Updates the content of a FlowFile by evaluating a Regular Expression against it and replacing the section of "
        + "the content that matches the Regular Expression with some alternate value.")
public class ReplaceText extends AbstractProcessor {

    //Constants
    public static final String LINE_BY_LINE = "Line-by-Line";
    public static final String ENTIRE_TEXT = "Entire text";
    private final Pattern backReferencePattern = Pattern.compile("\\$(\\d+)");
    private static final byte[] ZERO_BYTE_BUFFER = new byte[0];
    private static final String DEFAULT_REGEX = "(?s:^.*$)";
    private static final String DEFAULT_REPLACEMENT_VALUE = "$1";

    // Properties
    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("Regular Expression")
            .description("The Regular Expression to search for in the FlowFile content")
            .required(true)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .expressionLanguageSupported(true)
            .defaultValue(DEFAULT_REGEX)
            .build();
    public static final PropertyDescriptor REPLACEMENT_VALUE = new PropertyDescriptor.Builder()
            .name("Replacement Value")
            .description("The value to replace the regular expression with. Back-references to Regular Expression capturing groups are supported, but "
                    + "back-references that reference capturing groups that do not exist in the regular expression will be treated as literal value.")
            .required(true)
            .defaultValue(DEFAULT_REPLACEMENT_VALUE)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Buffer Size")
            .description("Specifies the maximum amount of data to buffer (per file or per line, depending on the Evaluation Mode) in order to "
                    + "apply the regular expressions. If 'Entire Text' (in Evaluation Mode) is selected and the FlowFile is larger than this value, "
                    + "the FlowFile will be routed to 'failure'. "
                    + "In 'Line-by-Line' Mode, if a single line is larger than this value, the FlowFile will be routed to 'failure'. A default value "
                    + "of 1 MB is provided, primarily for 'Entire Text' mode. In 'Line-by-Line' Mode, a value such as 8 KB or 16 KB is suggested. "
                    + "This value is ignored and the buffer is not used if 'Regular Expression' is set to '.*'")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();
    public static final PropertyDescriptor EVALUATION_MODE = new PropertyDescriptor.Builder()
            .name("Evaluation Mode")
            .description("Evaluate the 'Regular Expression' against each line (Line-by-Line) or buffer the entire file into memory (Entire Text) and "
                    + "then evaluate the 'Regular Expression'.")
            .allowableValues(LINE_BY_LINE, ENTIRE_TEXT)
            .defaultValue(ENTIRE_TEXT)
            .required(true)
            .build();
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that have been successfully updated are routed to this relationship, as well as FlowFiles whose content does not "
                    + "match the given Regular Expression")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be updated are routed to this relationship")
            .build();
    //
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REGEX);
        properties.add(REPLACEMENT_VALUE);
        properties.add(CHARACTER_SET);
        properties.add(MAX_BUFFER_SIZE);
        properties.add(EVALUATION_MODE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 100));
        if (flowFiles.isEmpty()) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final String unsubstitutedRegex = context.getProperty(REGEX).getValue();
        String unsubstitutedReplacement = context.getProperty(REPLACEMENT_VALUE).getValue();
        if (unsubstitutedRegex.equals(DEFAULT_REGEX) && unsubstitutedReplacement.equals(DEFAULT_REPLACEMENT_VALUE)) {
            // This pattern says replace content with itself. We can highly optimize this process by simply transferring
            // all FlowFiles to the 'success' relationship
            session.transfer(flowFiles, REL_SUCCESS);
            return;
        }

        final AttributeValueDecorator quotedAttributeDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                return Pattern.quote(attributeValue);
            }
        };

        final AttributeValueDecorator escapeBackRefDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                return attributeValue.replace("$", "\\$");
            }
        };

        final String regexValue = context.getProperty(REGEX).evaluateAttributeExpressions().getValue();
        final int numCapturingGroups = Pattern.compile(regexValue).matcher("").groupCount();

        final boolean skipBuffer = ".*".equals(unsubstitutedRegex);

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        final byte[] buffer = skipBuffer ? ZERO_BYTE_BUFFER : new byte[maxBufferSize];

        final String evaluateMode = context.getProperty(EVALUATION_MODE).getValue();

        for (FlowFile flowFile : flowFiles) {
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                if (flowFile.getSize() > maxBufferSize && !skipBuffer) {
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                }
            }

            String replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile, escapeBackRefDecorator).getValue();
            final Matcher backRefMatcher = backReferencePattern.matcher(replacement);
            while (backRefMatcher.find()) {
                final String backRefNum = backRefMatcher.group(1);
                if (backRefNum.startsWith("0")) {
                    continue;
                }
                final int originalBackRefIndex = Integer.parseInt(backRefNum);
                int backRefIndex = originalBackRefIndex;

                // if we have a replacement value like $123, and we have less than 123 capturing groups, then
                // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
                // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
                // we want to truncate the 1 and get 0.
                while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                    backRefIndex /= 10;
                }

                if (backRefIndex > numCapturingGroups) {
                    final StringBuilder sb = new StringBuilder(replacement.length() + 1);
                    final int groupStart = backRefMatcher.start(1);

                    sb.append(replacement.substring(0, groupStart - 1));
                    sb.append("\\");
                    sb.append(replacement.substring(groupStart - 1));
                    replacement = sb.toString();
                }
            }

            replacement = replacement.replaceAll("(\\$\\D)", "\\\\$1");

            // always match; just overwrite value with the replacement value; this optimization prevents us
            // from reading the file at all.
            final String replacementValue = replacement;
            if (skipBuffer) {
                final StopWatch stopWatch = new StopWatch(true);
                if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(replacementValue.getBytes(charset));
                        }
                    });
                } else {
                    flowFile = session.write(flowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {
                            try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset));) {
                                while (null != br.readLine()) {
                                    bw.write(replacementValue);
                                }
                            }
                        }
                    });
                }
                session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("Transferred {} to 'success'", new Object[]{flowFile});
                continue;
            }

            final StopWatch stopWatch = new StopWatch(true);
            final String regex = context.getProperty(REGEX).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                final int flowFileSize = (int) flowFile.getSize();
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        StreamUtils.fillBuffer(in, buffer, false);
                        final String contentString = new String(buffer, 0, flowFileSize, charset);
                        final String updatedValue = contentString.replaceAll(regex, replacementValue);
                        out.write(updatedValue.getBytes(charset));
                    }
                });
            } else {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset));) {
                            String oneLine;
                            while (null != (oneLine = br.readLine())) {
                                final String updatedValue = oneLine.replaceAll(regex, replacementValue);
                                bw.write(updatedValue);
                            }
                        }
                    }
                });
            }

            logger.info("Transferred {} to 'success'", new Object[]{flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}
