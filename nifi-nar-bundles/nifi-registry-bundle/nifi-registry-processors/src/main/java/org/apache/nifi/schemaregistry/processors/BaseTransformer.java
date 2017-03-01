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
package org.apache.nifi.schemaregistry.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

/**
 * Base processor which contains common functionality for processors that
 * receive {@link FlowFile} and output {@link FlowFile} and contain only two
 * {@link Relationship}s (i.e., success and failure). Every successful execution
 * of
 * {@link #doTransform(ProcessContext, ProcessSession, FlowFile, InvocationContextProperties)}
 * operation will result in transferring {@link FlowFile} to 'success'
 * relationship while any exception will result in such file going to 'failure'.
 */
public abstract class BaseTransformer extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully retrieved schema from Schema Registry")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to find a schema are sent to this relationship")
            .build();

    private static final Set<Relationship> BASE_RELATIONSHIPS;

    static {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        BASE_RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private final Map<PropertyDescriptor, String> propertyInstanceValues = new HashMap<>();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            try {
                InvocationContextProperties contextProperties = new InvocationContextProperties(context, flowFile);
                flowFile = this.doTransform(context, session, flowFile, contextProperties);
                session.transfer(flowFile, REL_SUCCESS);
            } catch (Exception e) {
                this.getLogger().error("Failed FlowFile processing, routing to failure. Issue: " + e.getMessage(), e);
                session.transfer(flowFile, REL_FAILURE);
            }
        } else {
            context.yield();
        }
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        List<PropertyDescriptor> propertyDescriptors = this.getSupportedPropertyDescriptors();
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (!propertyDescriptor.isExpressionLanguageSupported()){
                this.propertyInstanceValues.put(propertyDescriptor, context.getProperty(propertyDescriptor).getValue());
            }
        }
    }

    /**
     *
     */
    protected FlowFile doTransform(ProcessContext context, ProcessSession session, FlowFile flowFile,  InvocationContextProperties contextProperties) {
        AtomicReference<Map<String, String>> attributeRef = new AtomicReference<Map<String, String>>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                attributeRef.set(transform(in, null, contextProperties));
            }
        });
        if (attributeRef.get() != null) {
            flowFile = session.putAllAttributes(flowFile, attributeRef.get());
        }
        return flowFile;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return BASE_RELATIONSHIPS;
    }

    /**
     * This operation is designed to allow sub-classes to provide
     * implementations that read content of the provided {@link InputStream} and
     * write content (same or different) it into the provided
     * {@link OutputStream}. Both {@link InputStream} and {@link OutputStream}
     * represent the content of the in/out {@link FlowFile}. The
     * {@link OutputStream} can be null if no output needs to be written.
     * <p>
     * The returned {@link Map} represents attributes that will be added to the
     * outgoing FlowFile.
     *
     *
     * @param in
     *            {@link InputStream} representing data to be transformed
     * @param out
     *            {@link OutputStream} representing target stream to wrote
     *            transformed data. Can be null if no output needs to be
     *            written.
     * @param contextProperties
     *            instance of {@link InvocationContextProperties}
     */
    protected abstract Map<String, String> transform(InputStream in, OutputStream out, InvocationContextProperties contextProperties);

    /**
     * Properties object that gathers the value of the
     * {@link PropertyDescriptor} within the context of
     * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory)}
     * invocation. It maintains the knowledge of instance properties vs.
     * invocation properties that the values of which are set by evaluating
     * expression against the incoming {@link FlowFile}.
     */
    public class InvocationContextProperties {
        private final Map<PropertyDescriptor, String> propertyInvocationValues = new HashMap<>();

        InvocationContextProperties(ProcessContext context, FlowFile flowFile) {
            List<PropertyDescriptor> propertyDescriptors = BaseTransformer.this.getSupportedPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                if (propertyDescriptor.isExpressionLanguageSupported()) {
                    PropertyValue value = context.getProperty(propertyDescriptor)
                            .evaluateAttributeExpressions(flowFile);
                    this.propertyInvocationValues.put(propertyDescriptor, value.getValue());
                }
            }
        }

        /**
         * Returns the value of the property within the context of
         * {@link Processor#onTrigger(ProcessContext, ProcessSessionFactory)}
         * invocation.
         */
        public String getPropertyValue(PropertyDescriptor propertyDescriptor, boolean notNull) {
            String propertyValue = propertyInstanceValues.containsKey(propertyDescriptor)
                    ? propertyInstanceValues.get(propertyDescriptor)
                            : propertyInvocationValues.get(propertyDescriptor);
            if (notNull && propertyValue == null) {
                throw new IllegalArgumentException("Property '" + propertyDescriptor + "' evaluatd to null");
            }
            return propertyValue;
        }

        @Override
        public String toString() {
            return "Instance: " + propertyInstanceValues + "; Invocation: " + propertyInvocationValues;
        }
    }
}
