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
package org.apache.nifi.processors.morphlines;

import com.google.common.base.Preconditions;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import org.kitesdk.morphline.base.Notifications;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.lifecycle.OnScheduled;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.*;
import java.util.concurrent.atomic.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.exception.*;

@Tags({"kitesdk", "morphlines"})
@CapabilityDescription("Provide a description")
public class MorphlinesProcessor extends AbstractProcessor {

    private static Command morphline;
    private volatile Record record = new Record();
    private static volatile Collector collector = new Collector();

    public static final PropertyDescriptor MORPHLINES_ID = new PropertyDescriptor
            .Builder().name("Morphlines ID")
            .description("Identifier of the morphlines context")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor MORPHLINES_FILE = new PropertyDescriptor
            .Builder().name("Morphlines File")
            .description("File for the morphlines context")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor MORPHLINES_OUTPUT_FIELD = new PropertyDescriptor
            .Builder().name("Morphlines output field")
            .description("Field name of output in Morphlines. Default is '_attachment_body'.")
            .required(false)
            .defaultValue("_attachment_body")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for success.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Relationship for failure of morphlines.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Relationship for original flowfiles.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = ImmutableList.<PropertyDescriptor>builder()
            .add(MORPHLINES_FILE)
            .add(MORPHLINES_ID)
            .add(MORPHLINES_OUTPUT_FIELD)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = ImmutableSet.<Relationship>builder()
            .add(REL_SUCCESS)
            .add(REL_FAILURE)
            .build();

    private File morphLineFile;
    private String morphLinesId;
    private String morphlinesOutputField;
    private MorphlineContext morphlineContext;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws Exception {
        morphLineFile = new File(context.getProperty(MORPHLINES_FILE).getValue());
        morphLinesId = context.getProperty(MORPHLINES_ID).getValue();
        morphlinesOutputField = context.getProperty(MORPHLINES_OUTPUT_FIELD).getValue();
        morphlineContext = new MorphlineContext.Builder().build();
    }

    @OnStopped
    public void onStopped() {
        bufferQueue.clear();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final AtomicLong written = new AtomicLong(0L);
        final byte[] value = new byte[(int) flowFile.getSize()];

        try{
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    StreamUtils.fillBuffer(in, value);
                    Record record = new Record();
                    record.put(Fields.ATTACHMENT_BODY, value);
                    Collector collectorRecord = new Collector();
                    morphline = new org.kitesdk.morphline.base.Compiler().compile(morphLineFile, morphLinesId, morphlineContext, collectorRecord);
                    if (morphline.process(record)) {
                        List<Record> results = collectorRecord.getRecords();
                        for (Iterator<Record> it = results.iterator(); it.hasNext();) {
                            String outputValue = it.next().getFirstValue(morphlinesOutputField).toString() + "/n";
                            out.write(outputValue.getBytes());
                            written.incrementAndGet();
                        }
                    }
                }
            });

            if (written.get() > 0L) {
                // false to only update if file transfer is successful
                session.adjustCounter("Processed records in morphlines", written.get(), false);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (ProcessException e) {
            System.out.println("Error while processing the flowFile through Morphlines");
            session.transfer(flowFile, REL_FAILURE);
            throw e;
        }

    }

    private static final class Collector implements Command {

        private volatile List<Record> results = new ArrayList();

        public List<Record> getRecords() {
            return results;
        }

        public void reset() {
            results.clear();
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public boolean process(Record record) {
            Preconditions.checkNotNull(record);
            try {
                results.add(record);
            } catch (Exception e) {
                throw e;
            }
            return true;
        }

        public int getRecordCount() {
            return results.size();
        }
    }
}
