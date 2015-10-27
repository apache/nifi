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
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
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
@CapabilityDescription("Updates the content of a FlowFile by evaluating a Regular Expression (regex) against it and replacing the section of "
    + "the content that matches the Regular Expression with some alternate value.")
public class ReplaceText extends AbstractProcessor {

    // Constants
    public static final String LINE_BY_LINE = "Line-by-Line";
    public static final String ENTIRE_TEXT = "Entire text";
    public static final String prependValue = "Prepend";
    public static final String appendValue = "Append";
    public static final String regexReplaceValue = "Regex Replace";
    public static final String literalReplaceValue = "Literal Replace";
    private final Pattern backReferencePattern = Pattern.compile("\\$(\\d+)");
    private static final String DEFAULT_REGEX = "(?s:^.*$)";
    private static final String DEFAULT_REPLACEMENT_VALUE = "$1";

    // Prepend and Append will just insert the replacement value at the beginning or end
    // Properties PREPEND, APPEND, REGEX_REPLACE, LITERAL_REPLACE
    static final AllowableValue PREPEND = new AllowableValue(prependValue, prependValue,
        "Insert the Replacement Value at the beginning of the FlowFile or the beginning of each line (depending on the Evaluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be prepended to each line. For \"Entire Text\" evaluation mode, the value will be prepended to the entire text.");
    static final AllowableValue APPEND = new AllowableValue(appendValue, appendValue,
        "Insert the Replacement Value at the end of the FlowFile or the end of each line (depending on the Evluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be appended to each line. For \"Entire Text\" evaluation mode, the value will be appended to the entire text.");
    static final AllowableValue LITERAL_REPLACE = new AllowableValue(literalReplaceValue, literalReplaceValue,
        "Search for all instances of the Search Value and replace the matches with the Replacement Value.");
    static final AllowableValue REGEX_REPLACE = new AllowableValue(regexReplaceValue, regexReplaceValue,
        "Interpret the Search Value as a Regular Expression and replace all matches with the Replacement Value. The Replacement Value may reference Capturing Groups used "
            + "in the Search Value by using a dollar-sign followed by the Capturing Group number, such as $1 or $2. If the Search Value is set to .* then everything is replaced without "
            + "even evaluating the Regular Expression.");

    public static final PropertyDescriptor SEARCH_VALUE = new PropertyDescriptor.Builder()
        .name("Regular Expression")
        .displayName("Search Value")
        .description("The Search Value to search for in the FlowFile content. Only used for 'Literal Replace' and 'Regex Replace' matching strategies")
        .required(true)
        .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULT_REGEX)
        .build();
    public static final PropertyDescriptor REPLACEMENT_VALUE = new PropertyDescriptor.Builder()
        .name("Replacement Value")
        .description("The value to insert using the 'Replacement Strategy'. Using \"Regex Replace\" back-references to Regular Expression capturing groups "
            + "are supported, but back-references that reference capturing groups that do not exist in the regular expression will be treated as literal value.")
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
    public static final PropertyDescriptor REPLACEMENT_STRATEGY = new PropertyDescriptor.Builder()
        .name("Replacement Strategy")
        .description("The strategy for how and what to replace within the FlowFile's text content.")
        .allowableValues(PREPEND, APPEND, REGEX_REPLACE, LITERAL_REPLACE)
        .defaultValue(REGEX_REPLACE.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor EVALUATION_MODE = new PropertyDescriptor.Builder()
        .name("Evaluation Mode")
        .description("Run the 'Replacement Strategy' against each line separately (Line-by-Line) or buffer the entire file into memory (Entire Text) "
            + "and run against that.")
        .allowableValues(LINE_BY_LINE, ENTIRE_TEXT)
        .defaultValue(ENTIRE_TEXT)
        .required(true)
        .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that have been successfully processed are routed to this relationship. This includes both FlowFiles that had text"
            + " replaced and those that did not.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that could not be updated are routed to this relationship")
        .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SEARCH_VALUE);
        properties.add(REPLACEMENT_VALUE);
        properties.add(CHARACTER_SET);
        properties.add(MAX_BUFFER_SIZE);
        properties.add(REPLACEMENT_STRATEGY);
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

        final String unsubstitutedRegex = context.getProperty(SEARCH_VALUE).getValue();
        String unsubstitutedReplacement = context.getProperty(REPLACEMENT_VALUE).getValue();
        final String replacementStrategy = context.getProperty(REPLACEMENT_STRATEGY).getValue();

        if (replacementStrategy.equalsIgnoreCase(regexReplaceValue) && unsubstitutedRegex.equals(DEFAULT_REGEX) && unsubstitutedReplacement.equals(DEFAULT_REPLACEMENT_VALUE)) {
            // This pattern says replace content with itself. We can highly optimize this process by simply transferring
            // all FlowFiles to the 'success' relationship
            session.transfer(flowFiles, REL_SUCCESS);
            return;
        }

        final AttributeValueDecorator escapeBackRefDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                return attributeValue.replace("$", "\\$");
            }
        };

        final String regexValue = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions().getValue();
        final int numCapturingGroups = Pattern.compile(regexValue).matcher("").groupCount();

        final boolean skipBuffer = ".*".equals(unsubstitutedRegex);

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        final String evaluateMode = context.getProperty(EVALUATION_MODE).getValue();
        final byte[] buffer;
        if (replacementStrategy.equalsIgnoreCase(regexReplaceValue) || replacementStrategy.equalsIgnoreCase(literalReplaceValue)) {
            buffer = new byte[maxBufferSize];
        } else {
            buffer = null;
        }

        for (FlowFile flowFile : flowFiles) {
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                if (flowFile.getSize() > maxBufferSize && !skipBuffer) {
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                }
            }

            String replacement;
            if (!replacementStrategy.equals(regexReplaceValue)) {
                replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            } else {
                replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile, escapeBackRefDecorator).getValue();
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
            }

            ReplacementStrategyExecutor replacementStrategyExecutor;
            switch (replacementStrategy) {
                case prependValue:
                    replacementStrategyExecutor = new PrependReplace();
                    break;
                case appendValue:
                    replacementStrategyExecutor = new AppendReplace();
                    break;
                case regexReplaceValue:
                    replacementStrategyExecutor = new RegexReplace(buffer);
                    break;
                case literalReplaceValue:
                    replacementStrategyExecutor = new LiteralReplace(buffer);
                    break;
                default:
                    throw new AssertionError();
            }

            final StopWatch stopWatch = new StopWatch(true);

            flowFile = replacementStrategyExecutor.replace(flowFile, session, context, replacement, evaluateMode,
                charset, maxBufferSize, skipBuffer);

            logger.info("Transferred {} to 'success'", new Object[] {flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private static class PrependReplace implements ReplacementStrategyExecutor {

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String replacementValue, final String evaluateMode,
            final Charset charset, final int maxBufferSize, final boolean skipBuffer) {
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        out.write(replacementValue.getBytes(charset));
                        IOUtils.copy(in, out);
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
                                final String updatedValue = replacementValue.concat(oneLine);
                                bw.write(updatedValue);
                            }
                        }
                    }
                });
            }
            return flowFile;
        }
    }

    private static class AppendReplace implements ReplacementStrategyExecutor {

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String replacementValue, final String evaluateMode,
            final Charset charset, final int maxBufferSize, final boolean skipBuffer) {
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        IOUtils.copy(in, out);
                        out.write(replacementValue.getBytes(charset));
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
                                // we need to find the first carriage return or new-line so that we can append the new value
                                // before the line separate. However, we don't want to do this using a regular expression due
                                // to performance concerns. So we will find the first occurrence of either \r or \n and use
                                // that to insert the replacement value.
                                boolean foundNewLine = false;
                                for (int i = 0; i < oneLine.length(); i++) {
                                    final char c = oneLine.charAt(i);
                                    if (foundNewLine) {
                                        bw.write(c);
                                        continue;
                                    }

                                    if (c == '\r' || c == '\n') {
                                        bw.write(replacementValue);
                                        foundNewLine = true;
                                    }

                                    bw.write(c);
                                }

                                if (!foundNewLine) {
                                    bw.write(replacementValue);
                                }
                            }
                        }
                    }
                });
            }
            return flowFile;
        }
    }


    private static class RegexReplace implements ReplacementStrategyExecutor {
        private final byte[] buffer;

        public RegexReplace(final byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String replacementValue, final String evaluateMode,
            final Charset charset, final int maxBufferSize, final boolean skipBuffer) {
            final String replacementFinal = replacementValue.replaceAll("(\\$\\D)", "\\\\$1");

            // always match; just overwrite value with the replacement value; this optimization prevents us
            // from reading the file at all.
            if (skipBuffer) {
                if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(replacementFinal.getBytes(charset));
                        }
                    });
                } else {
                    flowFile = session.write(flowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {
                            try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset));) {
                                while (null != br.readLine()) {
                                    bw.write(replacementFinal);
                                }
                            }
                        }
                    });
                }
            } else {
                final AttributeValueDecorator quotedAttributeDecorator = new AttributeValueDecorator() {
                    @Override
                    public String decorate(final String attributeValue) {
                        return Pattern.quote(attributeValue);
                    }
                };
                final String searchRegex = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();

                final int flowFileSize = (int) flowFile.getSize();
                if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                    flowFile = session.write(flowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {
                            StreamUtils.fillBuffer(in, buffer, false);
                            final String contentString = new String(buffer, 0, flowFileSize, charset);
                            final String updatedValue = contentString.replaceAll(searchRegex, replacementFinal);
                            out.write(updatedValue.getBytes(charset));
                        }
                    });
                } else {
                    flowFile = session.write(flowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {
                            try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset))) {
                                String oneLine;
                                while (null != (oneLine = br.readLine())) {
                                    final String updatedValue = oneLine.replaceAll(searchRegex, replacementFinal);
                                    bw.write(updatedValue);
                                }
                            }
                        }
                    });
                }
            }
            return flowFile;
        }
    }

    private static class LiteralReplace implements ReplacementStrategyExecutor {
        private final byte[] buffer;

        public LiteralReplace(final byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String replacementValue, final String evaluateMode,
            final Charset charset, final int maxBufferSize, final boolean skipBuffer) {
            final AttributeValueDecorator quotedAttributeDecorator = new AttributeValueDecorator() {
                @Override
                public String decorate(final String attributeValue) {
                    return Pattern.quote(attributeValue);
                }
            };

            final String searchValue = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();


            final int flowFileSize = (int) flowFile.getSize();
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        StreamUtils.fillBuffer(in, buffer, false);
                        final String contentString = new String(buffer, 0, flowFileSize, charset);
                        // Interpreting the search and replacement values as char sequences
                        final String updatedValue = contentString.replace(searchValue, replacementValue);
                        out.write(updatedValue.getBytes(charset));
                    }
                });
            } else {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset))) {
                            String oneLine;
                            while (null != (oneLine = br.readLine())) {
                                // Interpreting the search and replacement values as char sequences
                                final String updatedValue = oneLine.replace(searchValue, replacementValue);
                                bw.write(updatedValue);
                            }
                        }
                    }
                });
            }
            return flowFile;
        }
    }

    private interface ReplacementStrategyExecutor {
        FlowFile replace(FlowFile flowFile, ProcessSession session, ProcessContext context, String replacement, String evaluateMode, Charset charset, int maxBufferSize, boolean skipBuffer);
    }
}
