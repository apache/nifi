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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.EventDrivenSourceRunner;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SchedulingContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * This processor runs a Flume source
 */
@Tags({"flume", "hadoop", "get", "source"})
@CapabilityDescription("Generate FlowFile data from a Flume source")
public class FlumeSourceProcessor extends AbstractFlumeProcessor {

    private Source source;
    private SourceRunner runner;
    private MemoryChannel channel;

    public static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor.Builder()
            .name("Source Type")
            .description("The fully-qualified name of the Source class")
            .required(true)
            .addValidator(createSourceValidator())
            .build();
    public static final PropertyDescriptor AGENT_NAME = new PropertyDescriptor.Builder()
            .name("Agent Name")
            .description("The name of the agent used in the Flume source configuration")
            .required(true)
            .defaultValue("tier1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SOURCE_NAME = new PropertyDescriptor.Builder()
            .name("Source Name")
            .description("The name of the source used in the Flume source configuration")
            .required(true)
            .defaultValue("src-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FLUME_CONFIG = new PropertyDescriptor.Builder()
            .name("Flume Configuration")
            .description("The Flume configuration for the source copied from the flume.properties file")
            .required(true)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(SOURCE_TYPE, AGENT_NAME, SOURCE_NAME, FLUME_CONFIG);
        this.relationships = ImmutableSet.of(SUCCESS);
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
    public void onScheduled(final SchedulingContext context) {
        try {
            source = SOURCE_FACTORY.create(
                    context.getProperty(SOURCE_NAME).getValue(),
                    context.getProperty(SOURCE_TYPE).getValue());

            String flumeConfig = context.getProperty(FLUME_CONFIG).getValue();
            String agentName = context.getProperty(AGENT_NAME).getValue();
            String sourceName = context.getProperty(SOURCE_NAME).getValue();
            Configurables.configure(source,
                    getFlumeSourceContext(flumeConfig, agentName, sourceName));

            if (source instanceof EventDrivenSource) {
                runner = new EventDrivenSourceRunner();
                channel = new MemoryChannel();
                Configurables.configure(channel, new Context());
                channel.start();
                source.setChannelProcessor(new ChannelProcessor(new NifiChannelSelector(channel)));
                runner.setSource(source);
                runner.start();
            }
        } catch (Throwable th) {
            getLogger().error("Error creating source", th);
            throw Throwables.propagate(th);
        }
    }

    @OnUnscheduled
    public void unScheduled() {
        if (runner != null) {
            runner.stop();
        }
        if (channel != null) {
            channel.stop();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context,
            final ProcessSession session) throws ProcessException {
        if (source instanceof EventDrivenSource) {
            onEventDrivenTrigger(context, session);
        } else if (source instanceof PollableSource) {
            onPollableTrigger((PollableSource) source, context, session);
        }
    }

    public void onPollableTrigger(final PollableSource pollableSource,
            final ProcessContext context, final ProcessSession session)
            throws ProcessException {
        try {
            pollableSource.setChannelProcessor(new ChannelProcessor(
                    new NifiChannelSelector(new NifiChannel(session, SUCCESS))));
            pollableSource.start();
            pollableSource.process();
            pollableSource.stop();
        } catch (EventDeliveryException ex) {
            throw new ProcessException("Error processing pollable source", ex);
        }
    }

    public void onEventDrivenTrigger(final ProcessContext context, final ProcessSession session) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        try {
            Event event = channel.take();
            if (event != null) {
                transferEvent(event, session, SUCCESS);
            }
            transaction.commit();
        } catch (Throwable th) {
            transaction.rollback();
            throw Throwables.propagate(th);
        } finally {
            transaction.close();
        }
    }

}
