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
import org.apache.nifi.components.ValidationContext;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    "notation of the matched content")
@WritesAttributes({
    @WritesAttribute(attribute = "grok.XXX", description = "When operating in flowfile-attribute mode, each of the Grok identifier that is matched in the flowfile " +
        "will be added as an attribute, prefixed with \"grok.\" For example," +
        "if the grok identifier \"timestamp\" is matched, then the value will be added to an attribute named \"grok.timestamp\"")})
public class ExtractGrok extends AbstractProcessor {

    public static final String FLOWFILE_ATTRIBUTE = "flowfile-attribute";
    public static final String FLOWFILE_CONTENT = "flowfile-content";
    private static final String APPLICATION_JSON = "application/json";

    public static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .description("Grok expression")
        .required(true)
        .addValidator(validateGrokExpression())
        .build();

    public static final PropertyDescriptor GROK_PATTERN_FILE = new PropertyDescriptor.Builder()
        .name("Grok Pattern file")
        .description("Grok Pattern file definition")
        .required(true)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
        .name("Destination")
        .description("Control if Grok output value is written as a new flowfile attributes, in this case " +
            "each of the Grok identifier that is matched in the flowfile will be added as an attribute, " +
            "prefixed with \"grok.\" or written in the flowfile content. Writing to flowfile content " +
            "will overwrite any existing flowfile content.")
        .required(true)
        .allowableValues(FLOWFILE_ATTRIBUTE, FLOWFILE_CONTENT)
        .defaultValue(FLOWFILE_ATTRIBUTE)
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
        .description("Specifies the maximum amount of data to buffer (per file) in order to apply the Grok expressions. Files larger than the specified maximum will not be fully evaluated.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
        .defaultValue("1 MB")
        .build();

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

    private volatile Grok grok = new Grok();
    private final BlockingQueue<byte[]> bufferQueue = new LinkedBlockingQueue<>();

    static {
        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_MATCH);
        _relationships.add(REL_NO_MATCH);
        relationships = Collections.unmodifiableSet(_relationships);

        final List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.add(GROK_EXPRESSION);
        _descriptors.add(GROK_PATTERN_FILE);
        _descriptors.add(DESTINATION);
        _descriptors.add(CHARACTER_SET);
        _descriptors.add(MAX_BUFFER_SIZE);
        descriptors = Collections.unmodifiableList(_descriptors);
    }

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
        for (int i = 0; i < context.getMaxConcurrentTasks(); i++) {
            final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final byte[] buffer = new byte[maxBufferSize];
            bufferQueue.add(buffer);
        }

        grok = new Grok();
        grok.addPatternFromFile(context.getProperty(GROK_PATTERN_FILE).getValue());
        grok.compile(context.getProperty(GROK_EXPRESSION).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
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

        final Match gm = grok.match(contentString);
        gm.captures();

        if (gm.toMap().isEmpty()) {
            session.transfer(flowFile, REL_NO_MATCH);
            getLogger().info("Did not match any Grok Expressions for FlowFile {}", new Object[]{flowFile});
            return;
        }

        final ObjectMapper objectMapper = new ObjectMapper();
        switch (context.getProperty(DESTINATION).getValue()) {
            case FLOWFILE_ATTRIBUTE:
                Map<String, String> grokResults = new HashMap<>();
                for (Map.Entry<String, Object> entry : gm.toMap().entrySet()) {
                    if (null != entry.getValue()) {
                        grokResults.put("grok." + entry.getKey(), entry.getValue().toString());
                    }
                }

                flowFile = session.putAllAttributes(flowFile, grokResults);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_MATCH);
                getLogger().info("Matched {} Grok Expressions and added attributes to FlowFile {}", new Object[]{grokResults.size(), flowFile});

                break;
            case FLOWFILE_CONTENT:
                FlowFile conFlowfile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        out.write(objectMapper.writeValueAsBytes(gm.toMap()));
                    }
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                session.getProvenanceReporter().modifyContent(conFlowfile, "Replaced content with parsed Grok fields and values", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(conFlowfile, REL_MATCH);

                break;
        }
    }


    public static final Validator validateGrokExpression() {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {

                Grok grok = new Grok();
                try {
                    grok.compile(input);
                } catch (GrokException | java.util.regex.PatternSyntaxException e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Not a valid Grok Expression - " + e.getMessage())
                            .build();
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };
    }

}
