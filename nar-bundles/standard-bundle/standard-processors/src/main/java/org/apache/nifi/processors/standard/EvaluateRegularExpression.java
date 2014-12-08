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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.StreamUtils;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.lang.StringUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"evaluate", "Text", "Regular Expression", "regex", "experimental"})
@CapabilityDescription(
        "Evaluates one or more Regular Expressions against the content of a FlowFile.  "
        + "The results of those Regular Expressions are assigned to FlowFile Attributes.  "
        + "Regular Expressions are entered by adding user-defined properties; "
        + "the name of the property maps to the Attribute Name into which the result will be placed.  "
        + "The value of the property must be a valid Regular Expressions with exactly one capturing group.  "
        + "If the Regular Expression matches more than once, only the first match will be used.  "
        + "If any provided Regular Expression matches, the FlowFile(s) will be routed to 'matched'. "
        + "If no provided Regular Expression matches, the FlowFile will be routed to 'unmatched' and no attributes will be applied to the FlowFile.")

public class EvaluateRegularExpression extends AbstractProcessor {

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
            .defaultValue("1 MB")
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
            .description("Indicates that '^' and '$' should match just after and just before a line terminator or end of sequence, instead of only the begining or end of the entire input.  Can also be specified via the embeded flag (?m).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNICODE_CASE = new PropertyDescriptor.Builder()
            .name("Enable Unicode-aware Case Folding")
            .description("When used with 'Enable Case-insensitive Matching', matches in a manner consistent with the Unicode Standard.  Can also be specified via the embeded flag (?u).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNICODE_CHARACTER_CLASS = new PropertyDescriptor.Builder()
            .name("Enable Unicode Predefined Character Classes")
            .description("Specifies conformance with the Unicode Technical Standard #18: Unicode Regular Expression Annex C: Compatibility Properties.  Can also be specified via the embeded flag (?U).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor UNIX_LINES = new PropertyDescriptor.Builder()
            .name("Enable Unix Lines Mode")
            .description("Indicates that only the '\n' line terminator is recognized int the behavior of '.', '^', and '$'.  Can also be specified via the embeded flag (?d).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description(
                    "FlowFiles are routed to this relationship when the Regular Expression is successfully evaluated and the FlowFile "
                    + "is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description(
                    "FlowFiles are routed to this relationship when no provided Regular Expression matches the content of the FlowFile")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CHARACTER_SET);
        properties.add(MAX_BUFFER_SIZE);
        properties.add(CANON_EQ);
        properties.add(CASE_INSENSITIVE);
        properties.add(COMMENTS);
        properties.add(DOTALL);
        properties.add(LITERAL);
        properties.add(MULTILINE);
        properties.add(UNICODE_CASE);
        properties.add(UNICODE_CHARACTER_CLASS);
        properties.add(UNIX_LINES);
        this.properties = Collections.unmodifiableList(properties);
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
                .addValidator(StandardValidators.createRegexValidator(1, 1, true))
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFileBatch = session.get(50);
        if (flowFileBatch.isEmpty()) {
            return;
        }
        final ProcessorLog logger = getLogger();

        // Compile the Regular Expressions
        Map<String, Matcher> regexMap = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final int flags = getCompileFlags(context);
            final Matcher matcher = Pattern.compile(entry.getValue(), flags).matcher("");
            regexMap.put(entry.getKey().getName(), matcher);
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        for (FlowFile flowFile : flowFileBatch) {

            final Map<String, String> regexResults = new HashMap<>();

            final byte[] buffer = new byte[maxBufferSize];

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, buffer, false);
                }
            });

            final int flowFileSize = Math.min((int) flowFile.getSize(), maxBufferSize);

            final String contentString = new String(buffer, 0, flowFileSize, charset);

            for (final Map.Entry<String, Matcher> entry : regexMap.entrySet()) {

                final Matcher matcher = entry.getValue();

                matcher.reset(contentString);

                if (matcher.find()) {
                    final String group = matcher.group(1);
                    if (!StringUtils.isBlank(group)) {
                        regexResults.put(entry.getKey(), group);
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

        } // end flowFileLoop
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
