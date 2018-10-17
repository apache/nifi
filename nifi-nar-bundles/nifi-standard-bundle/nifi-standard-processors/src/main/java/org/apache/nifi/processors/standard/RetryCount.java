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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.processors.standard.RetryCount.RETRY_COUNT_ATTRIBUTE_KEY;
import static org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_REQUIRED;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

/**
 * Created by jpercivall on 3/17/17.
 */
@SupportsBatching
@SideEffectFree
@InputRequirement(INPUT_REQUIRED)
@Tags({"penalize", "retry", "penalize"})
@CapabilityDescription("Used to facilitate retrying a FlowFile a set number of times before considering the FlowFile 'failed'. Can also be used as a utility to penalize FlowFiles.")
@WritesAttribute(attribute = RETRY_COUNT_ATTRIBUTE_KEY, description = "The number of times this FlowFile has been routed to 'Retry'.")
@ReadsAttribute(attribute = RETRY_COUNT_ATTRIBUTE_KEY, description = "The number of times this FlowFile has been routed to 'Retry'.")
public class RetryCount extends AbstractProcessor {

    public static final String RETRY_COUNT_ATTRIBUTE_KEY = "retryCountProcessor.timesRetried";

    public static final PropertyDescriptor PROP_RETRY_LIMIT = new PropertyDescriptor.Builder()
            .name("Retry limit")
            .displayName("Retry limit")
            .description("The number of times to retry the FlowFile before considering it 'failed' and routing it to 'over limit'.")
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor PROP_PENALIZE_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Penalize FlowFile")
            .displayName("Penalize FlowFile")
            .description("If true then the FlowFiles routed to 'retry' will be penalized.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor PROP_WARN_ON_OVER_LIMIT = new PropertyDescriptor.Builder()
            .name("Warn on 'over limit'")
            .displayName("Warn on 'over limit'")
            .description("If true then when a FlowFile is routed to 'over limit' a message will be logged at the level 'warn'.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor PROP_INPUT_QUEUE_DATA_SIZE_LIMIT = new PropertyDescriptor.Builder()
            .name("Input queue data size limit")
            .displayName("Input queue data size limit")
            .description("If the total input queue data size (a combination of all connections feeding this processor) is above this configured limit then any processed FlowFile is routed to " +
                    "'over limit'. This is necessary to avoid a back-pressure deadlock caused by with circular back-pressure. The default values are set to be half of what the default " +
                    "configuration values are for connections back-pressure.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .defaultValue("500 mb")
            .build();

    public static final PropertyDescriptor PROP_INPUT_QUEUE_COUNT_LIMIT = new PropertyDescriptor.Builder()
            .name("Input queue count limit")
            .displayName("Input queue count limit")
            .description("If the total input queue FlowFile count (a combination of all connections feeding this processor) is above this configured limit then any processed FlowFile is routed to " +
                    "'over limit'. This is necessary to avoid a back-pressure deadlock caused by with circular back-pressure. The default values are set to be half of what the default " +
                    "configuration values are for connections back-pressure.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .defaultValue("5000")
            .build();


    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles that have not yet exceeded the retry limit are routed here.")
            .build();

    public static final Relationship REL_OVER_LIMIT = new Relationship.Builder()
            .name("over limit")
            .description("A FlowFile which has exceeded the limit for the number of retries is routed here.")
            .build();


    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> descriptorsTemp = new ArrayList<PropertyDescriptor>();
        descriptorsTemp.add(PROP_RETRY_LIMIT);
        descriptorsTemp.add(PROP_PENALIZE_FLOWFILE);
        descriptorsTemp.add(PROP_WARN_ON_OVER_LIMIT);
        descriptorsTemp.add(PROP_INPUT_QUEUE_DATA_SIZE_LIMIT);
        descriptorsTemp.add(PROP_INPUT_QUEUE_COUNT_LIMIT);
        descriptors = Collections.unmodifiableList(descriptorsTemp);

        final Set<Relationship> relationshipsTemp = new HashSet<Relationship>();
        relationshipsTemp.add(REL_OVER_LIMIT);
        relationshipsTemp.add(REL_RETRY);
        relationships = relationshipsTemp;
    }

    private volatile boolean penalize;
    private volatile boolean warnOnOverLimit;
    private volatile int retryLimit;
    private volatile double dataSizeLimit;
    private volatile int objectCountLimit;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void setPropertyConstants(final ProcessContext context) {
        penalize = context.getProperty(PROP_PENALIZE_FLOWFILE).asBoolean();
        warnOnOverLimit = context.getProperty(PROP_WARN_ON_OVER_LIMIT).asBoolean();
        retryLimit = context.getProperty(PROP_RETRY_LIMIT).evaluateAttributeExpressions().asInteger();
        dataSizeLimit = context.getProperty(PROP_INPUT_QUEUE_DATA_SIZE_LIMIT).evaluateAttributeExpressions().asDataSize(DataUnit.B);
        objectCountLimit = context.getProperty(PROP_INPUT_QUEUE_COUNT_LIMIT).evaluateAttributeExpressions().asInteger();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Optional<String> timesRetriedStringOptional = Optional.ofNullable(flowFile.getAttribute(RETRY_COUNT_ATTRIBUTE_KEY));

        final int timesRetried = Integer.parseInt(timesRetriedStringOptional.orElse("0"));

        if (timesRetried >= retryLimit) {

            if (warnOnOverLimit) {
                getLogger().warn("The FlowFile {} has been retried {} times and will be routed to 'over limit'", new Object[]{flowFile, timesRetried});
            }
            session.transfer(flowFile, REL_OVER_LIMIT);

        } else {

            int inputObjectCount = session.getQueueSize().getObjectCount();
            double inputDataSize = session.getQueueSize().getByteCount();

            // Check if the input queue has grown too large and we shouldn't retry
            if (inputDataSize > dataSizeLimit) {
                getLogger().error("The input queue data size has grown too large and the FlowFile {} will be routed to 'over limit'. The FlowFile was only retried {} time(s). " +
                        "The queue size is {} B.", new Object[]{flowFile, timesRetried, inputDataSize});
                session.transfer(flowFile, REL_OVER_LIMIT);
                return;
            } else if (inputObjectCount > objectCountLimit) {
                getLogger().error("The input queue object count has grown too large and the FlowFile {} will be routed to 'over limit'. The FlowFile was only retried {} time(s). " +
                        "The queue count is {}.", new Object[]{flowFile, timesRetried, inputObjectCount});
                session.transfer(flowFile, REL_OVER_LIMIT);
                return;
            }

            flowFile = session.putAttribute(flowFile, RETRY_COUNT_ATTRIBUTE_KEY, String.valueOf(timesRetried + 1));

            if (penalize) {
                flowFile = session.penalize(flowFile);
            }
            session.transfer(flowFile, REL_RETRY);
        }
    }
}
