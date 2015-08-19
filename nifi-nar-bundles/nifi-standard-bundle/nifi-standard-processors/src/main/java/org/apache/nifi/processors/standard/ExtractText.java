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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"evaluate", "extract", "Text", "Regular Expression", "regex"})
@CapabilityDescription(
        "Evaluates one or more Regular Expressions against the content of a FlowFile.  "
        + "The results of those Regular Expressions are assigned to FlowFile Attributes.  "
        + "Regular Expressions are entered by adding user-defined properties; "
        + "the name of the property maps to the Attribute Name into which the result will be placed.  "
        + "The first capture group, if any found, will be placed into that attribute name."
        + "But all capture groups, including the matching string sequence itself will also be "
        + "provided at that attribute name with an index value provided."
        + "The value of the property must be a valid Regular Expressions with one or more capturing groups.  "
        + "If the Regular Expression matches more than once, only the first match will be used.  "
        + "If any provided Regular Expression matches, the FlowFile(s) will be routed to 'matched'. "
        + "If no provided Regular Expression matches, the FlowFile will be routed to 'unmatched' "
        + "and no attributes will be applied to the FlowFile.")
@DynamicProperty(name = "A FlowFile attribute", value = "A Regular Expression with one or more capturing group",
        description = "The first capture group, if any found, will be placed into that attribute name."
        + "But all capture groups, including the matching string sequence itself will also be "
        + "provided at that attribute name with an index value provided.")
public class ExtractText extends AbstractProcessor {

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Buffer Size")
            .description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions.  Files larger than the specified maximum will not be fully evaluated.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor MAX_CAPTURE_GROUP_LENGTH = new PropertyDescriptor.Builder()
            .name("Maximum Capture Group Length")
            .description("Specifies the maximum number of characters a given capture group value can have.  Any characters beyond the max will be truncated.")
            .required(false)
            .defaultValue("1024")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CANON_EQ = new PropertyDescriptor.Builder()
            .name("Enable Canonical Equivalence")
            .description("Indicates that two characters match only when their full canonical decompositions match.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CASE_INSENSITIVE = new PropertyDescriptor.Builder()
            .name("Enable Case-insensitive Matching")
            .description("Indicates that two characters match even if they are in a different case.  Can also be specified via the embeded flag (?i).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMMENTS = new PropertyDescriptor.Builder()
            .name("Permit Whitespace and Comments in Pattern")
            .description("In this mode, whitespace is ignored, and embedded comments starting with # are ignored until the end of a line.  Can also be specified via the embeded flag (?x).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor DOTALL = new PropertyDescriptor.Builder()
            .name("Enable DOTALL Mode")
            .description("Indicates that the expression '.' should match any character, including a line terminator.  Can also be specified via the embeded flag (?s).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor LITERAL = new PropertyDescriptor.Builder()
            .name("Enable Literal Parsing of the Pattern")
            .description("Indicates that Metacharacters and escape characters should be given no special meaning.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MULTILINE = new PropertyDescriptor.Builder()
            .name("Enable Multiline Mode")
            .description("Indicates that '^' and '$' should match just after and just before a line terminator or end of sequence, instead of "
                    + "only the begining or end of the entire input.  Can also be specified via the embeded flag (?m).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNICODE_CASE = new PropertyDescriptor.Builder()
            .name("Enable Unicode-aware Case Folding")
            .description("When used with 'Enable Case-insensitive Matching', matches in a manner consistent with the Unicode Standard.  Can also "
                    + "be specified via the embeded flag (?u).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNICODE_CHARACTER_CLASS = new PropertyDescriptor.Builder()
            .name("Enable Unicode Predefined Character Classes")
            .description("Specifies conformance with the Unicode Technical Standard #18: Unicode Regular Expression Annex C: Compatibility "
                    + "Properties.  Can also be specified via the embeded flag (?U).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNIX_LINES = new PropertyDescriptor.Builder()
            .name("Enable Unix Lines Mode")
            .description("Indicates that only the '\n' line terminator is recognized in the behavior of '.', '^', and '$'.  Can also be specified "
                    + "via the embeded flag (?d).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor INCLUDE_CAPTURE_GROUP_ZERO = new PropertyDescriptor.Builder()
            .name("Include Capture Group 0")
            .description("Indicates that Capture Group 0 should be included as an attribute. Capture Group 0 represents the entirety of the regular expression match, is typically not used, and "
                    + "could have considerable length.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the Regular Expression is successfully evaluated and the FlowFile is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when no provided Regular Expression matches the content of the FlowFile")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<Map<String, Pattern>> compiledPattersMapRef = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_MATCH);
        rels.add(REL_NO_MATCH);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CHARACTER_SET);
        props.add(MAX_BUFFER_SIZE);
        props.add(MAX_CAPTURE_GROUP_LENGTH);
        props.add(CANON_EQ);
        props.add(CASE_INSENSITIVE);
        props.add(COMMENTS);
        props.add(DOTALL);
        props.add(LITERAL);
        props.add(MULTILINE);
        props.add(UNICODE_CASE);
        props.add(UNICODE_CHARACTER_CLASS);
        props.add(UNIX_LINES);
        props.add(INCLUDE_CAPTURE_GROUP_ZERO);
        this.properties = Collections.unmodifiableList(props);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.createRegexValidator(1, 40, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        final Map<String, Pattern> compiledPatternsMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final int flags = getCompileFlags(context);
            final Pattern pattern = Pattern.compile(entry.getValue(), flags);
            compiledPatternsMap.put(entry.getKey().getName(), pattern);
        }
        compiledPattersMapRef.set(compiledPatternsMap);

        for (int i = 0; i < context.getMaxConcurrentTasks(); i++) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final byte[] buffer = new byte[maxBufferSize];
            bufferQueue.add(buffer);
        }
    }

    @OnStopped
    public void onStopped() {
        bufferQueue.clear();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ProcessorLog logger = getLogger();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxCaptureGroupLength = context.getProperty(MAX_CAPTURE_GROUP_LENGTH).asInteger();

        final String contentString;
        byte[] buffer = bufferQueue.poll();
        if (buffer == null) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            buffer = new byte[maxBufferSize];
        }

        try {
            final byte[] byteBuffer = buffer;
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, byteBuffer, false);
                }
            });

            final long len = Math.min(byteBuffer.length, flowFile.getSize());
            contentString = new String(byteBuffer, 0, (int) len, charset);
        } finally {
            bufferQueue.offer(buffer);
        }

        final Map<String, String> regexResults = new HashMap<>();

        final Map<String, Pattern> patternMap = compiledPattersMapRef.get();

        final int startGroupIdx = context.getProperty(INCLUDE_CAPTURE_GROUP_ZERO).asBoolean() ? 0 : 1;

        for (final Map.Entry<String, Pattern> entry : patternMap.entrySet()) {

            final Matcher matcher = entry.getValue().matcher(contentString);

            if (matcher.find()) {
                final String baseKey = entry.getKey();
                for (int i = startGroupIdx; i <= matcher.groupCount(); i++) {
                    final String key = new StringBuilder(baseKey).append(".").append(i).toString();
                    String value = matcher.group(i);
                    if (value.length() > maxCaptureGroupLength) {
                        value = value.substring(0, maxCaptureGroupLength);
                    }
                    regexResults.put(key, value);
                    if (i == 1) {
                        regexResults.put(baseKey, value);
                    }
                }
            }
        }

        if (!regexResults.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, regexResults);
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_MATCH);
            logger.info("Matched {} Regular Expressions and added attributes to FlowFile {}", new Object[]{regexResults.size(), flowFile});
        } else {
            session.transfer(flowFile, REL_NO_MATCH);
            logger.info("Did not match any Regular Expressions for  FlowFile {}", new Object[]{flowFile});
        }

    }

    int getCompileFlags(ProcessContext context) {
        int flags = (context.getProperty(UNIX_LINES).asBoolean() ? Pattern.UNIX_LINES : 0)
                | (context.getProperty(CASE_INSENSITIVE).asBoolean() ? Pattern.CASE_INSENSITIVE : 0)
                | (context.getProperty(COMMENTS).asBoolean() ? Pattern.COMMENTS : 0)
                | (context.getProperty(MULTILINE).asBoolean() ? Pattern.MULTILINE : 0)
                | (context.getProperty(LITERAL).asBoolean() ? Pattern.LITERAL : 0)
                | (context.getProperty(DOTALL).asBoolean() ? Pattern.DOTALL : 0)
                | (context.getProperty(UNICODE_CASE).asBoolean() ? Pattern.UNICODE_CASE : 0)
                | (context.getProperty(CANON_EQ).asBoolean() ? Pattern.CANON_EQ : 0)
                | (context.getProperty(UNICODE_CHARACTER_CLASS).asBoolean() ? Pattern.UNICODE_CHARACTER_CLASS : 0);
        return flags;
    }
}
