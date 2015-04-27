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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@Tags({"test", "random", "generate"})
@CapabilityDescription("This processor creates FlowFiles of random data and is used for load testing")
public class GenerateFlowFile extends AbstractProcessor {

    private final AtomicReference<byte[]> data = new AtomicReference<>();

    public static final String DATA_FORMAT_BINARY = "Binary";
    public static final String DATA_FORMAT_TEXT = "Text";

    public static final PropertyDescriptor FILE_SIZE = new PropertyDescriptor.Builder()
            .name("File Size")
            .description("The size of the file that will be used")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of FlowFiles to be transferred in each invocation")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .description("Specifies whether the data should be Text or Binary")
            .required(true)
            .defaultValue(DATA_FORMAT_BINARY)
            .allowableValues(DATA_FORMAT_BINARY, DATA_FORMAT_TEXT)
            .build();
    public static final PropertyDescriptor UNIQUE_FLOWFILES = new PropertyDescriptor.Builder()
            .name("Unique FlowFiles")
            .description("If true, each FlowFile that is generated will be unique. If false, a random value will be generated and all FlowFiles "
                    + "will get the same content but this offers much higher throughput")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private static final char[] TEXT_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()-_=+/?.,';:\"?<>\n\t ".toCharArray();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FILE_SIZE);
        descriptors.add(BATCH_SIZE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(UNIQUE_FLOWFILES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(UNIQUE_FLOWFILES).asBoolean()) {
            this.data.set(null);
        } else {
            this.data.set(generateData(context));
        }

    }

    private byte[] generateData(final ProcessContext context) {
        final int byteCount = context.getProperty(FILE_SIZE).asDataSize(DataUnit.B).intValue();

        final Random random = new Random();
        final byte[] array = new byte[byteCount];
        if (context.getProperty(DATA_FORMAT).getValue().equals(DATA_FORMAT_BINARY)) {
            random.nextBytes(array);
        } else {
            for (int i = 0; i < array.length; i++) {
                final int index = random.nextInt(TEXT_CHARS.length);
                array[i] = (byte) TEXT_CHARS[index];
            }
        }

        return array;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final byte[] data;
        if (context.getProperty(UNIQUE_FLOWFILES).asBoolean()) {
            data = generateData(context);
        } else {
            data = this.data.get();
        }

        for (int i = 0; i < context.getProperty(BATCH_SIZE).
                asInteger(); i++) {
            FlowFile flowFile = session.create();
            if (data.length > 0) {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(data);
                    }
                });
            }

            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
        }
    }
}
