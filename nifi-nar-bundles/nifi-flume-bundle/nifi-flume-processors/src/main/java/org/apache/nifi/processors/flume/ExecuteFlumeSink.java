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
package org.apache.nifi.processors.flume;

import java.util.List;
import java.util.Set;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.conf.Configurables;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * This processor runs a Flume sink
 */
@TriggerSerially
@Tags({"flume", "hadoop", "put", "sink"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Execute a Flume sink. Each input FlowFile is converted into a Flume Event for processing by the sink.")
public class ExecuteFlumeSink extends AbstractFlumeProcessor {

    public static final PropertyDescriptor SINK_TYPE = new PropertyDescriptor.Builder()
            .name("Sink Type")
            .description("The component type name for the sink. For some sinks, this is a short, symbolic name (e.g. hdfs)."
                    + " For others, it's the fully-qualified name of the Sink class. See the Flume User Guide for details.")
            .required(true)
            .addValidator(createSinkValidator())
            .build();
    public static final PropertyDescriptor AGENT_NAME = new PropertyDescriptor.Builder()
            .name("Agent Name")
            .description("The name of the agent used in the Flume sink configuration")
            .required(true)
            .defaultValue("tier1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SOURCE_NAME = new PropertyDescriptor.Builder()
            .name("Sink Name")
            .description("The name of the sink used in the Flume sink configuration")
            .required(true)
            .defaultValue("sink-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FLUME_CONFIG = new PropertyDescriptor.Builder()
            .name("Flume Configuration")
            .description("The Flume configuration for the sink copied from the flume.properties file")
            .required(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile Sink sink;
    private volatile NifiSinkSessionChannel channel;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(SINK_TYPE, AGENT_NAME, SOURCE_NAME, FLUME_CONFIG);
        this.relationships = ImmutableSet.of(SUCCESS, FAILURE);
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
        try {
            channel = new NifiSinkSessionChannel(SUCCESS, FAILURE);
            channel.start();

            sink = SINK_FACTORY.create(context.getProperty(SOURCE_NAME).getValue(),
                    context.getProperty(SINK_TYPE).getValue());
            sink.setChannel(channel);

            String flumeConfig = context.getProperty(FLUME_CONFIG).getValue();
            String agentName = context.getProperty(AGENT_NAME).getValue();
            String sinkName = context.getProperty(SOURCE_NAME).getValue();
            Configurables.configure(sink,
                    getFlumeSinkContext(flumeConfig, agentName, sinkName));

            sink.start();
        } catch (Throwable th) {
            getLogger().error("Error creating sink", th);
            throw Throwables.propagate(th);
        }
    }

    @OnStopped
    public void stopped() {
        sink.stop();
        channel.stop();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        channel.setSession(session);
        try {
            sink.process();
        } catch (EventDeliveryException ex) {
            throw new ProcessException("Flume event delivery failed", ex);
        }
    }
}
