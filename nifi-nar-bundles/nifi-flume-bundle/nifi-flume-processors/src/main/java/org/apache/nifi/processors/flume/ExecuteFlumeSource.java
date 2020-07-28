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
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.Source;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.EventDrivenSourceRunner;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This processor runs a Flume source
 */
@TriggerSerially
@Tags({"flume", "hadoop", "get", "source"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Execute a Flume source. Each Flume Event is sent to the success relationship as a FlowFile")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary Flume configurations assuming all permissions that NiFi has.")
        }
)
public class ExecuteFlumeSource extends AbstractFlumeProcessor {

    public static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor.Builder()
        .name("Source Type")
        .description("The component type name for the source. For some sources, this is a short, symbolic name"
                + " (e.g. spooldir). For others, it's the fully-qualified name of the Source class. See the Flume User Guide for details.")
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

    public static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile Source source;

    private final NifiSessionChannel pollableSourceChannel = new NifiSessionChannel(SUCCESS);
    private final AtomicReference<ProcessSessionFactory> sessionFactoryRef = new AtomicReference<>(null);
    private final AtomicReference<EventDrivenSourceRunner> runnerRef = new AtomicReference<>(null);
    private final AtomicReference<NifiSessionFactoryChannel> eventDrivenSourceChannelRef = new AtomicReference<>(null);

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
    public void onScheduled(final ProcessContext context) {
        try {
            source = SOURCE_FACTORY.create(
                    context.getProperty(SOURCE_NAME).getValue(),
                    context.getProperty(SOURCE_TYPE).getValue());

            String flumeConfig = context.getProperty(FLUME_CONFIG).getValue();
            String agentName = context.getProperty(AGENT_NAME).getValue();
            String sourceName = context.getProperty(SOURCE_NAME).getValue();
            Configurables.configure(source,
                getFlumeSourceContext(flumeConfig, agentName, sourceName));

            if (source instanceof PollableSource) {
                source.setChannelProcessor(new ChannelProcessor(
                    new NifiChannelSelector(pollableSourceChannel)));
                source.start();
            }
        } catch (Throwable th) {
            getLogger().error("Error creating source", th);
            throw Throwables.propagate(th);
        }
    }

    @OnStopped
    public void stopped() {
        if (source instanceof PollableSource) {
            source.stop();
        } else {
            EventDrivenSourceRunner runner = runnerRef.get();
            if (runner != null) {
                runner.stop();
                runnerRef.compareAndSet(runner, null);
            }

            NifiSessionFactoryChannel eventDrivenSourceChannel = eventDrivenSourceChannelRef.get();
            if (eventDrivenSourceChannel != null) {
                eventDrivenSourceChannel.stop();
                eventDrivenSourceChannelRef.compareAndSet(eventDrivenSourceChannel, null);
            }
        }
        sessionFactoryRef.set(null);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (source instanceof PollableSource) {
            super.onTrigger(context, sessionFactory);
        } else if (source instanceof EventDrivenSource) {
            ProcessSessionFactory old = sessionFactoryRef.getAndSet(sessionFactory);
            if (old != sessionFactory) {
                if (runnerRef.get() != null) {
                    stopped();
                    sessionFactoryRef.set(sessionFactory);
                }

                runnerRef.set(new EventDrivenSourceRunner());
                eventDrivenSourceChannelRef.set(new NifiSessionFactoryChannel(sessionFactoryRef.get(), SUCCESS));
                eventDrivenSourceChannelRef.get().start();
                source.setChannelProcessor(new ChannelProcessor(
                    new NifiChannelSelector(eventDrivenSourceChannelRef.get())));
                runnerRef.get().setSource(source);
                runnerRef.get().start();
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (source instanceof PollableSource) {
            PollableSource pollableSource = (PollableSource) source;
            try {
                pollableSourceChannel.setSession(session);
                pollableSource.process();
            } catch (EventDeliveryException ex) {
                throw new ProcessException("Error processing pollable source", ex);
            }
        } else {
            throw new ProcessException("Invalid source type: " + source.getClass().getSimpleName());
        }
    }
}
