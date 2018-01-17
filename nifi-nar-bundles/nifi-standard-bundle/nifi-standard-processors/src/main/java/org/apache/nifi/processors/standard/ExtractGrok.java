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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.FlowFile;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@EventDriven
@SupportsBatching
@SideEffectFree
@Tags({"grok", "log", "text", "parse", "delimit", "extract"})
@CapabilityDescription("Evaluates one or more Grok Expressions against the content of a FlowFile, " +
    "adding the results as attributes or replacing the content of the FlowFile with a JSON " +
    "notation of the matched content\n" +
        "uses https://github.com/thekrakken/java-grok.")
@WritesAttributes({
    @WritesAttribute(attribute = "{result prefix}XXX", description = "When operating in flowfile-attribute mode, each of the Grok identifier that is matched in the flowfile " +
        "will be added as an attribute, prefixed with \"{result prefix}\" For example," +
        "if the grok identifier \"timestamp\" is matched, then the value will be added to an attribute named \"{result prefix}timestamp\""),

        @WritesAttribute(attribute = "ExtractGrok.exception", description = "if an error occurs, an exception will be written to this attribute, " +
                "and the flow routed to 'unmatched' ")
})
public class ExtractGrok extends AbstractProcessor {

    public static final String FLOWFILE_ATTRIBUTE = "flowfile-attribute";
    public static final String FLOWFILE_CONTENT = "flowfile-content";
    public static final String APPLICATION_JSON = "application/json";
    public static final String GROK_EXPRESSION_KEY = "Grok Expression";
    public static final String GROK_PATTERN_FILE_KEY = "Grok Pattern file";
    public static final String DESTINATION_KEY = "Destination";
    public static final String CHARACTER_SET_KEY = "Character Set";
    public static final String MAXIMUM_BUFFER_SIZE_KEY = "Maximum Buffer Size";
    public static final String NAMED_CAPTURES_ONLY_KEY = "Named captures only";
    public static final String SINGLE_MATCH_KEY = "Single Match";
    public static final String RESULT_PREFIX_KEY = "result prefix";
    public static final String MATCHED_EXP_ATTR_KEY = "matched expression attribute";
    public static final String EXP_SEPARATOR_KEY = "expression-separator";
    public static final String PATTERN_FILE_LIST_SEPARATOR = ",";

    //properties
    public static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name(GROK_EXPRESSION_KEY)
        .description("Grok expressions, one or more grok expressions separated by ',' or other character as set in attribute" + EXP_SEPARATOR_KEY)
        .required(true)
        .addValidator(validateGrokExpression())
        .build();


    public static final PropertyDescriptor GROK_PATTERN_FILE = new PropertyDescriptor.Builder()
        .name(GROK_PATTERN_FILE_KEY)
        .description("Grok Pattern file definition. May include multiple files, separated by " + PATTERN_FILE_LIST_SEPARATOR)
        .required(true)
        .addValidator(validateMultipleFilesExist())
        .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
        .name(DESTINATION_KEY)
        .description("Control if Grok output value is written as a new flowfile attributes, in this case " +
            "each of the Grok identifier that is matched in the flowfile will be added as an attribute, " +
            "prefixed with \"grok.\" or written in the flowfile content. Writing to flowfile content " +
            "will overwrite any existing flowfile content.")
        .required(true)
        .allowableValues(FLOWFILE_ATTRIBUTE, FLOWFILE_CONTENT)
        .defaultValue(FLOWFILE_ATTRIBUTE)
        .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name(CHARACTER_SET_KEY)
        .description("The Character Set in which the file is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name(MAXIMUM_BUFFER_SIZE_KEY)
        .description("Specifies the maximum amount of data to buffer (per file) in order to apply the Grok expressions. Files larger than the specified maximum will not be fully evaluated.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
        .defaultValue("1 MB")
        .build();

     public static final PropertyDescriptor NAMED_CAPTURES_ONLY = new PropertyDescriptor.Builder()
        .name(NAMED_CAPTURES_ONLY_KEY)
        .description("Only store named captures from grokList")
        .required(true)
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor BREAK_ON_FIRST_MATCH = new PropertyDescriptor.Builder()
        .name(SINGLE_MATCH_KEY)
        .description("Stop on first matched expression.")
        .required(true)
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build();

    public static final PropertyDescriptor RESULT_PREFIX = new PropertyDescriptor.Builder()
        .name(RESULT_PREFIX_KEY)
        .description("Value to prefix attribute results with (avoid collisions with existing properties)" +
                "\n\t (Does not apply when results returned as content)" +
                "\n\t (May be empty, the dot (.) separator is not implied)")
        .required(true)
        .defaultValue("grok.")
        .addValidator(Validator.VALID)
        .build();


    public static final PropertyDescriptor EXPRESSION_SEPARATOR = new PropertyDescriptor.Builder()
        .name(EXP_SEPARATOR_KEY)
        .description("character to use to separate multiple grok expressions ")
        .required(true)
        .defaultValue(",")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();


    public static final PropertyDescriptor MATCHED_EXP_ATTR = new PropertyDescriptor.Builder()
        .name(MATCHED_EXP_ATTR_KEY)
        .description("Name of attribute to receive the name(s) of the matched expression(s).")
        .required(true)
        .defaultValue("matched_expression")
        .addValidator(Validator.VALID)
        .build();

    // relationships

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the Grok Expression is successfully evaluated and the FlowFile is modified as a result")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when no provided Grok Expression matches the content of the FlowFile")
            .build();


    private final static List<PropertyDescriptor> descriptors;
    private final static Set<Relationship> relationships;

    private volatile List<Grok> grokList = new ArrayList<>();
    private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();

    static {
        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_MATCH);
        _relationships.add(REL_NO_MATCH);
        relationships = Collections.unmodifiableSet(_relationships);

        final List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.add(GROK_EXPRESSION);
        _descriptors.add(EXPRESSION_SEPARATOR);
        _descriptors.add(GROK_PATTERN_FILE);
        _descriptors.add(DESTINATION);
        _descriptors.add(CHARACTER_SET);
        _descriptors.add(MAX_BUFFER_SIZE);
        _descriptors.add(NAMED_CAPTURES_ONLY);
        _descriptors.add(RESULT_PREFIX);
        _descriptors.add(BREAK_ON_FIRST_MATCH);
        _descriptors.add(MATCHED_EXP_ATTR);
        descriptors = Collections.unmodifiableList(_descriptors);
    }

    private String resultPrefix = "";
    private boolean breakOnFirstMatch;
    private String matchedExpressionAttribute;
    private String expressionSeparator;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnStopped
    public void onStopped() {
        bufferQueue.clear();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GrokException {
        grokList.clear();
        for (int i = 0; i < context.getMaxConcurrentTasks(); i++) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final byte[] buffer = new byte[maxBufferSize];
            bufferQueue.add(buffer);
        }

        resultPrefix = context.getProperty(RESULT_PREFIX).getValue();
        breakOnFirstMatch = context.getProperty(BREAK_ON_FIRST_MATCH).asBoolean() ;
        matchedExpressionAttribute = context.getProperty(MATCHED_EXP_ATTR).getValue();
        expressionSeparator = context.getProperty(EXPRESSION_SEPARATOR).getValue();

        String patterns  = context.getProperty(GROK_EXPRESSION).getValue();
        for (String patternName : patterns.split(expressionSeparator)) {
            Grok grok = new Grok();
            final String patternFileListString = context.getProperty(GROK_PATTERN_FILE).getValue();
            for (String patternFile : patternFileListString.split(PATTERN_FILE_LIST_SEPARATOR)) {
                grok.addPatternFromFile(patternFile);
            }
          grok.compile(patternName, context.getProperty(NAMED_CAPTURES_ONLY).asBoolean());
          grokList.add(grok);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        Map<String, Object> results = new HashMap<>();
        List<String> matchedExpressionList = new ArrayList<>();

        final StopWatch stopWatch = new StopWatch(true);
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String contentString;
        byte[] buffer = bufferQueue.poll();
        if (buffer == null) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            buffer = new byte[maxBufferSize];
        }

        try {
            final byte[] byteBuffer = buffer;
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, byteBuffer, false));
            final long len = Math.min(byteBuffer.length, flowFile.getSize());
            contentString = new String(byteBuffer, 0, (int) len, charset);
        } finally {
            bufferQueue.offer(buffer);
        }

        try{
            for (Grok grok : grokList) {
                final Match gm = grok.match(contentString);
                gm.captures();
                final Map<String, Object> localResults = gm.toMap();
                if (!localResults.isEmpty()) {
                    matchedExpressionList.add(grok.getOriginalGrokPattern());
                    results.putAll(localResults);
                    if (breakOnFirstMatch) {
                        break;
                    }
                }
            }

            if (results.isEmpty()) {
                session.transfer(flowFile, REL_NO_MATCH);
                getLogger().debug("Did not match any Grok Expression for FlowFile {}", new Object[]{flowFile});
                return;
            }

            String matchedExpressions = StringUtils.join(matchedExpressionList, expressionSeparator);
            flowFile = session.putAttribute(flowFile, matchedExpressionAttribute, matchedExpressions);

            switch (context.getProperty(DESTINATION).getValue()) {
                case FLOWFILE_ATTRIBUTE:
                    Map<String, String> grokResults = new HashMap<>();
                    for (Map.Entry<String, Object> entry : results.entrySet()) {
                        if (null != entry.getValue()) {
                            grokResults.put(resultPrefix + entry.getKey(), entry.getValue().toString());
                        }
                    }
                    flowFile = session.putAllAttributes(flowFile, grokResults);
                    session.getProvenanceReporter().modifyAttributes(flowFile);
                    session.transfer(flowFile, REL_MATCH);
                    getLogger().info("Matched {} Grok Expressions and added attributes to FlowFile {}", new Object[]{grokResults.size(), flowFile});
                    break;
                case FLOWFILE_CONTENT:
                    final ObjectMapper objectMapper = new ObjectMapper();
                    FlowFile conFlowfile = session.write(flowFile, (in, out) -> {
                        out.write(objectMapper.writeValueAsBytes(results));
                    });

                    conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                    session.getProvenanceReporter().modifyContent(conFlowfile, "Replaced content with parsed Grok fields and values", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.transfer(conFlowfile, REL_MATCH);
                    break;
            }
        }catch(ProcessException t){
            flowFile = session.putAttribute(flowFile, getClass().getSimpleName() + ".exception", t.getMessage());
            session.transfer(flowFile, REL_NO_MATCH);
            getLogger().error("Did not match any Grok Expression for FlowFile {}", new Object[]{flowFile});
            return;
        }
    }

    public static Validator validateMultipleFilesExist() {
        return (subject, input, context) -> {
            for (String s : input.split(PATTERN_FILE_LIST_SEPARATOR)) {
                return StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, s, context);
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        };
    }

    public static Validator validateGrokExpression() {
        return (subject, input, context) -> {
            Grok grok = new Grok();
            String separator = context.getProperty(EXPRESSION_SEPARATOR).getValue();
            try {
                for (String s : input.split(separator)) {
                    grok.compile(s);
                }
            } catch (GrokException | java.util.regex.PatternSyntaxException e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Not a valid Grok Expression - " + e.getMessage())
                        .build();
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        };
    }

}
