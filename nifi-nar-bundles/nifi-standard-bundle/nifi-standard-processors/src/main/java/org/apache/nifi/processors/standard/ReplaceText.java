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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.NLKBufferedReader;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Text", "Regular Expression", "Update", "Change", "Replace", "Modify", "Regex"})
@CapabilityDescription("Updates the content of a FlowFile by evaluating a Regular Expression (regex) against it and replacing the section of "
    + "the content that matches the Regular Expression with some alternate value.")
public class ReplaceText extends AbstractProcessor {

    private static Pattern REPLACEMENT_NORMALIZATION_PATTERN = Pattern.compile("(\\$\\D)");

    // Constants
    public static final String LINE_BY_LINE = "Line-by-Line";
    public static final String ENTIRE_TEXT = "Entire text";
    public static final String prependValue = "Prepend";
    public static final String appendValue = "Append";
    public static final String regexReplaceValue = "Regex Replace";
    public static final String literalReplaceValue = "Literal Replace";
    public static final String alwaysReplace = "Always Replace";
    private static final Pattern unescapedBackReferencePattern = Pattern.compile("[^\\\\]\\$(\\d+)");
    private static final String DEFAULT_REGEX = "(?s)(^.*$)";
    private static final String DEFAULT_REPLACEMENT_VALUE = "$1";

    // Prepend and Append will just insert the replacement value at the beginning or end
    // Properties PREPEND, APPEND, REGEX_REPLACE, LITERAL_REPLACE
    static final AllowableValue PREPEND = new AllowableValue(prependValue, prependValue,
        "Insert the Replacement Value at the beginning of the FlowFile or the beginning of each line (depending on the Evaluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be prepended to each line. For \"Entire Text\" evaluation mode, the value will be prepended to the entire text.");
    static final AllowableValue APPEND = new AllowableValue(appendValue, appendValue,
        "Insert the Replacement Value at the end of the FlowFile or the end of each line (depending on the Evaluation Mode). For \"Line-by-Line\" Evaluation Mode, "
            + "the value will be appended to each line. For \"Entire Text\" evaluation mode, the value will be appended to the entire text.");
    static final AllowableValue LITERAL_REPLACE = new AllowableValue(literalReplaceValue, literalReplaceValue,
        "Search for all instances of the Search Value and replace the matches with the Replacement Value.");
    static final AllowableValue REGEX_REPLACE = new AllowableValue(regexReplaceValue, regexReplaceValue,
        "Interpret the Search Value as a Regular Expression and replace all matches with the Replacement Value. The Replacement Value may reference Capturing Groups used "
            + "in the Search Value by using a dollar-sign followed by the Capturing Group number, such as $1 or $2. If the Search Value is set to .* then everything is replaced without "
            + "even evaluating the Regular Expression.");
    static final AllowableValue ALWAYS_REPLACE = new AllowableValue(alwaysReplace, alwaysReplace,
        "Always replaces the entire line or the entire contents of the FlowFile (depending on the value of the <Evaluation Mode> property) and does not bother searching "
            + "for any value. When this strategy is chosen, the <Search Value> property is ignored.");

    public static final PropertyDescriptor SEARCH_VALUE = new PropertyDescriptor.Builder()
        .name("Regular Expression")
        .displayName("Search Value")
        .description("The Search Value to search for in the FlowFile content. Only used for 'Literal Replace' and 'Regex Replace' matching strategies")
        .required(true)
        .addValidator(Validator.VALID)
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULT_REGEX)
        .build();
    public static final PropertyDescriptor REPLACEMENT_VALUE = new PropertyDescriptor.Builder()
        .name("Replacement Value")
        .description("The value to insert using the 'Replacement Strategy'. Using \"Regex Replace\" back-references to Regular Expression capturing groups "
            + "are supported, but back-references that reference capturing groups that do not exist in the regular expression will be treated as literal value. "
            + "Back References may also be referenced using the Expression Language, as '$1', '$2', etc. The single-tick marks MUST be included, as these variables are "
            + "not \"Standard\" attribute names (attribute names must be quoted unless they contain only numbers, letters, and _).")
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
            + "apply the replacement. If 'Entire Text' (in Evaluation Mode) is selected and the FlowFile is larger than this value, "
            + "the FlowFile will be routed to 'failure'. "
            + "In 'Line-by-Line' Mode, if a single line is larger than this value, the FlowFile will be routed to 'failure'. A default value "
            + "of 1 MB is provided, primarily for 'Entire Text' mode. In 'Line-by-Line' Mode, a value such as 8 KB or 16 KB is suggested. "
            + "This value is ignored if the <Replacement Strategy> property is set to one of: Append, Prepend, Always Replace")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .build();
    public static final PropertyDescriptor REPLACEMENT_STRATEGY = new PropertyDescriptor.Builder()
        .name("Replacement Strategy")
        .description("The strategy for how and what to replace within the FlowFile's text content.")
        .allowableValues(PREPEND, APPEND, REGEX_REPLACE, LITERAL_REPLACE, ALWAYS_REPLACE)
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(validationContext));

        switch (validationContext.getProperty(REPLACEMENT_STRATEGY).getValue()) {
            case literalReplaceValue:
                errors.add(StandardValidators.NON_EMPTY_VALIDATOR
                        .validate(SEARCH_VALUE.getName(), validationContext.getProperty(SEARCH_VALUE).getValue(), validationContext));
                break;

            case regexReplaceValue:
                errors.add(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true)
                        .validate(SEARCH_VALUE.getName(), validationContext.getProperty(SEARCH_VALUE).getValue(), validationContext));
                break;

            case appendValue:
            case prependValue:
            case alwaysReplace:
            default:
                // nothing to check, search value is not used
                break;
        }

        return errors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 100));
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();

        final String replacementStrategy = context.getProperty(REPLACEMENT_STRATEGY).getValue();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        final String evaluateMode = context.getProperty(EVALUATION_MODE).getValue();
        final byte[] buffer;
        if (replacementStrategy.equalsIgnoreCase(regexReplaceValue) || replacementStrategy.equalsIgnoreCase(literalReplaceValue)) {
            buffer = new byte[maxBufferSize];
        } else {
            buffer = null;
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
                // for backward compatibility - if replacement regex is ".*" then we will simply always replace the content.
                if (context.getProperty(SEARCH_VALUE).getValue().equals(".*")) {
                    replacementStrategyExecutor = new AlwaysReplace();
                } else {
                    replacementStrategyExecutor = new RegexReplace(buffer, context);
                }

                break;
            case literalReplaceValue:
                replacementStrategyExecutor = new LiteralReplace(buffer);
                break;
            case alwaysReplace:
                replacementStrategyExecutor = new AlwaysReplace();
                break;
            default:
                throw new AssertionError();
        }

        for (FlowFile flowFile : flowFiles) {
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                if (flowFile.getSize() > maxBufferSize && replacementStrategyExecutor.isAllDataBufferedForEntireText()) {
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                }
            }

            final StopWatch stopWatch = new StopWatch(true);

            flowFile = replacementStrategyExecutor.replace(flowFile, session, context, evaluateMode, charset, maxBufferSize);

            logger.info("Transferred {} to 'success'", new Object[] {flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        }
    }


    // If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
    // groups and the Replacement Value has the value is "I owe $8 to him", then we want to treat the $8 as a literal "$8", rather
    // than attempting to use it as a back reference.
    private static String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
        if (numCapturingGroups == 0) {
            return unescaped;
        }

        String value = unescaped;
        final Matcher backRefMatcher = unescapedBackReferencePattern.matcher(value); // consider unescaped back references
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
                final StringBuilder sb = new StringBuilder(value.length() + 1);
                final int groupStart = backRefMatcher.start(1);

                sb.append(value.substring(0, groupStart - 1));
                sb.append("\\");
                sb.append(value.substring(groupStart - 1));
                value = sb.toString();
            }
        }

        return value;
    }

    private static class AlwaysReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {

            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            final StringBuilder lineEndingBuilder = new StringBuilder(2);

            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        out.write(replacementValue.getBytes(charset));
                    }
                });
            } else {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset));) {

                            String line;
                            while ((line = br.readLine()) != null) {
                                // We need to determine what line ending was used and use that after our replacement value.
                                lineEndingBuilder.setLength(0);
                                for (int i = line.length() - 1; i >= 0; i--) {
                                    final char c = line.charAt(i);
                                    if (c == '\r' || c == '\n') {
                                        lineEndingBuilder.append(c);
                                    } else {
                                        break;
                                    }
                                }

                                bw.write(replacementValue);

                                // Preserve original line endings. Reverse string because we iterated over original line ending in reverse order, appending to builder.
                                // So if builder has multiple characters, they are now reversed from the original string's ordering.
                                bw.write(lineEndingBuilder.reverse().toString());
                            }
                        }
                    }
                });
            }

            return flowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }
    }

    private static class PrependReplace implements ReplacementStrategyExecutor {
        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();

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

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }
    }

    private static class AppendReplace implements ReplacementStrategyExecutor {

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();

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

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return false;
        }
    }


    private static class RegexReplace implements ReplacementStrategyExecutor {
        private final byte[] buffer;
        private final int numCapturingGroups;
        private final Map<String, String> additionalAttrs;

        // back references are not supported in the evaluated expression
        private static final AttributeValueDecorator escapeBackRefDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                // when we encounter a '$[0-9+]'  replace it with '\$[0-9+]'
                return attributeValue.replaceAll("(\\$\\d+?)", "\\\\$1");
            }
        };

        public RegexReplace(final byte[] buffer, final ProcessContext context) {
            this.buffer = buffer;

            final String regexValue = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions().getValue();
            numCapturingGroups = Pattern.compile(regexValue).matcher("").groupCount();
            additionalAttrs = new HashMap<>(numCapturingGroups);
        }

        @Override
        public FlowFile replace(final FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {
            final AttributeValueDecorator quotedAttributeDecorator = new AttributeValueDecorator() {
                @Override
                public String decorate(final String attributeValue) {
                    return Pattern.quote(attributeValue);
                }
            };
            final String searchRegex = context.getProperty(SEARCH_VALUE).evaluateAttributeExpressions(flowFile, quotedAttributeDecorator).getValue();
            final Pattern searchPattern = Pattern.compile(searchRegex);

            final int flowFileSize = (int) flowFile.getSize();
            FlowFile updatedFlowFile;
            if (evaluateMode.equalsIgnoreCase(ENTIRE_TEXT)) {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        StreamUtils.fillBuffer(in, buffer, false);
                    }
                });

                final String contentString = new String(buffer, 0, flowFileSize, charset);
                additionalAttrs.clear();
                final Matcher matcher = searchPattern.matcher(contentString);
                if (matcher.find()) {
                    for (int i = 1; i <= matcher.groupCount(); i++) {
                        final String groupValue = matcher.group(i);
                        additionalAttrs.put("$" + i, groupValue);
                    }

                    String replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
                    replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);

                    String replacementFinal = normalizeReplacementString(replacement);

                    final String updatedValue = contentString.replaceAll(searchRegex, replacementFinal);
                    updatedFlowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(updatedValue.getBytes(charset));
                        }
                    });
                } else {
                    // If no match, just return the original. No need to write out any content.
                    return flowFile;
                }
            } else {
                updatedFlowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(final InputStream in, final OutputStream out) throws IOException {
                        try (NLKBufferedReader br = new NLKBufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charset))) {
                            String oneLine;
                            while (null != (oneLine = br.readLine())) {
                                additionalAttrs.clear();
                                final Matcher matcher = searchPattern.matcher(oneLine);
                                if (matcher.find()) {
                                    for (int i = 1; i <= matcher.groupCount(); i++) {
                                        final String groupValue = matcher.group(i);
                                        additionalAttrs.put("$" + i, groupValue);
                                    }

                                    String replacement = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
                                    replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);

                                    String replacementFinal = normalizeReplacementString(replacement);

                                    final String updatedValue = oneLine.replaceAll(searchRegex, replacementFinal);
                                    bw.write(updatedValue);
                                } else {
                                    // No match. Just write out the line as it was.
                                    bw.write(oneLine);
                                }
                            }
                        }
                    }
                });
            }

            return updatedFlowFile;
        }

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return true;
        }
    }

    private static class LiteralReplace implements ReplacementStrategyExecutor {
        private final byte[] buffer;

        public LiteralReplace(final byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public FlowFile replace(FlowFile flowFile, final ProcessSession session, final ProcessContext context, final String evaluateMode, final Charset charset, final int maxBufferSize) {

            final String replacementValue = context.getProperty(REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();

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

        @Override
        public boolean isAllDataBufferedForEntireText() {
            return true;
        }
    }

    /**
     * If we have a '$' followed by anything other than a number, then escape
     * it. E.g., '$d' becomes '\$d' so that it can be used as a literal in a
     * regex.
     */
    private static String normalizeReplacementString(String replacement) {
        String replacementFinal = replacement;
        if (REPLACEMENT_NORMALIZATION_PATTERN.matcher(replacement).find()) {
            replacementFinal = Matcher.quoteReplacement(replacement);
        }
        return replacementFinal;
    }

    private interface ReplacementStrategyExecutor {
        FlowFile replace(FlowFile flowFile, ProcessSession session, ProcessContext context, String evaluateMode, Charset charset, int maxBufferSize);

        boolean isAllDataBufferedForEntireText();
    }
}
