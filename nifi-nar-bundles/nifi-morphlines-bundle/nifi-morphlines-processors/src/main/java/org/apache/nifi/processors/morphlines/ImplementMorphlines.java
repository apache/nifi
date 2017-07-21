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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
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
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Notifications;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"kitesdk", "morphlines", "ETL", "HDFS", "avro", "Solr", "HBase"})
@CapabilityDescription("Implements Morphlines (http://kitesdk.org/docs/1.1.0/morphlines/) framework, which performs in-memory container of transformation "
    + "commands in oder to perform tasks such as loading, parsing, transforming, or otherwise processing a single record.")
@DynamicProperty(name = "Relationship Name", value = "A Regular Expression", supportsExpressionLanguage = true, description = "Adds the dynamic property key and value "
    + "as key-value pair to Morphlines content.")

public class ImplementMorphlines extends AbstractProcessor {
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

    public PropertyValue morphlinesFileProperty;
    public PropertyValue morphlinesIdProperty;
    public PropertyValue morphlinesOutputFieldProperty;
    public Map<String, PropertyValue> dynamicPropertyMap = new HashMap();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .required(false)
            .name(propertyDescriptorName)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamic(true)
            .expressionLanguageSupported(true)
            .build();
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws Exception {
        morphlinesFileProperty = context.getProperty(MORPHLINES_FILE);
        morphlinesIdProperty = context.getProperty(MORPHLINES_ID);
        morphlinesOutputFieldProperty = context.getProperty(MORPHLINES_OUTPUT_FIELD);
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                dynamicPropertyMap.put(descriptor.getName(), context.getProperty(descriptor));
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        FlowFile originalFlowFile = session.clone(flowFile);
        final AtomicLong written = new AtomicLong(0L);
        final byte[] value = new byte[(int) flowFile.getSize()];

        final File morphlinesFile = new File(morphlinesFileProperty.evaluateAttributeExpressions(flowFile).getValue());
        final String morphlinesId = morphlinesIdProperty.evaluateAttributeExpressions(flowFile).getValue();
        final String morphlinesOutputField = morphlinesOutputFieldProperty.evaluateAttributeExpressions(flowFile).getValue();
        Map<String, Object> settings = new HashMap();
        for (final String descriptorName : dynamicPropertyMap.keySet()) {
            final PropertyValue dynamicPropertyValue = dynamicPropertyMap.get(descriptorName);
            settings.put(descriptorName, dynamicPropertyValue.evaluateAttributeExpressions(flowFile).getValue());
        }
        final MorphlineContext morphlineContext = new MorphlineContext.Builder().setSettings(settings).build();

        final Collector collectorRecord = new Collector();
        final Command morphline = new Compiler().compile(morphlinesFile, morphlinesId, morphlineContext, collectorRecord);

        try{
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    Record record = new Record();
                    StreamUtils.fillBuffer(in, value);
                    record.put(Fields.ATTACHMENT_BODY, value);
                    Notifications.notifyStartSession(morphline);
                    if (morphline.process(record)) {
                        List<Record> results = collectorRecord.getRecords();
                        for (Iterator<Record> it = results.iterator(); it.hasNext();) {
                            Record result = it.next();
                            if (result.getFirstValue(morphlinesOutputField) != null) {
                                String outputValue = it.next().getFirstValue(morphlinesOutputField).toString() + "/n";
                                out.write(outputValue.getBytes());
                                written.incrementAndGet();
                            } else {
                                getLogger().warn(String.format("Unable to get %s within processed record: %s", morphlinesOutputField, result.toString()));
                            }
                        }
                        Notifications.notifyCommitTransaction(morphline);
                    }
                }
            });

            if (written.get() > 0L) {
                // false to only update if file transfer is successful
                session.adjustCounter("Processed records in morphlines", written.get(), false);
                session.transfer(flowFile, REL_SUCCESS);
                session.transfer(originalFlowFile, REL_ORIGINAL);
            } else {
                getLogger().warn(String.format("Morphlines transformations did not march any of the input records for %s Morphlines ID", morphlinesId));
                session.transfer(flowFile, REL_ORIGINAL);
                session.transfer(originalFlowFile, REL_FAILURE);
            }
        } catch (ProcessException e) {
            getLogger().error("Error while processing the flowFile through Morphlines");
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(originalFlowFile, REL_FAILURE);
            Notifications.notifyRollbackTransaction(morphline);
            morphlineContext.getExceptionHandler().handleException(e, null);
        }
        Notifications.notifyShutdown(morphline);
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
            results.add(record);
            return true;
        }

        public int getRecordCount() {
            return results.size();
        }
    }
}
