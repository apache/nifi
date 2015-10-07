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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.util.ValidatingBase64InputStream;
import org.apache.nifi.util.StopWatch;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"encode", "base64"})
@CapabilityDescription("Encodes or decodes content to and from base64")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class Base64EncodeContent extends AbstractProcessor {

    public static final String ENCODE_MODE = "Encode";
    public static final String DECODE_MODE = "Decode";

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
        .name("Mode")
        .description("Specifies whether the content should be encoded or decoded")
        .required(true)
        .allowableValues(ENCODE_MODE, DECODE_MODE)
        .defaultValue(ENCODE_MODE)
        .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully encoded or decoded will be routed to success")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be encoded or decoded will be routed to failure")
        .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        boolean encode = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCODE_MODE);
        try {
            final StopWatch stopWatch = new StopWatch(true);
            if (encode) {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        try (Base64OutputStream bos = new Base64OutputStream(out)) {
                            int len = -1;
                            byte[] buf = new byte[8192];
                            while ((len = in.read(buf)) > 0) {
                                bos.write(buf, 0, len);
                            }
                            bos.flush();
                        }
                    }
                });
            } else {
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        try (Base64InputStream bis = new Base64InputStream(new ValidatingBase64InputStream(in))) {
                            int len = -1;
                            byte[] buf = new byte[8192];
                            while ((len = bis.read(buf)) > 0) {
                                out.write(buf, 0, len);
                            }
                            out.flush();
                        }
                    }
                });
            }

            logger.info("Successfully {} {}", new Object[] {encode ? "encoded" : "decoded", flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ProcessException e) {
            logger.error("Failed to {} {} due to {}", new Object[] {encode ? "encode" : "decode", flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
