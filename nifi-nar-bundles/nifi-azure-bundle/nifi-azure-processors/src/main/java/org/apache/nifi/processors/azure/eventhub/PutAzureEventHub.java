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
package org.apache.nifi.processors.azure.eventhub;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubSender;


@SupportsBatching
@Tags({ "microsoft", "azure", "cloud", "eventhub", "events", "streams", "streaming" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends the contents of a FlowFile to a Windows Azure Event Hub. Note: the content of the FlowFile will be buffered into memory before being sent, "
    + "so care should be taken to avoid sending FlowFiles to this Processor that exceed the amount of Java Heap Space available.")
public class PutAzureEventHub extends AbstractProcessor {

    static final AllowableValue DELIVERY_MODE_PERSISTENT = new AllowableValue(String.valueOf(DeliveryMode.PERSISTENT), "Persistent", "This mode indicates that the Event Hub "
        + "server must persist the message to a reliable storage mechanism before the FlowFile is routed to 'success', in order to ensure that the data is not lost.");

    static final AllowableValue DELIVERY_MODE_NON_PERSISTENT = new AllowableValue(String.valueOf(DeliveryMode.NON_PERSISTENT), "Non-Persistent",
        "This mode indicates that the Event Hub server does not have to persist the message to a reliable storage mechanism before the FlowFile is routed to 'success'. "
            + "This delivery mode may offer higher throughput but may result in message loss if the server crashes or is restarted.");

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
        .name("Event Hub Name")
        .description("The name of the Azure Event Hub to send to")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();
    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
        .name("Event Hub Namespace")
        .description("The Azure Namespace that the Event Hub is assigned to. This is generally equal to <Event Hub Name>-ns")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(true)
        .build();
    static final PropertyDescriptor ACCESS_POLICY = new PropertyDescriptor.Builder()
        .name("Shared Access Policy Name")
        .description("The name of the Event Hub Shared Access Policy. This Policy must have Send permissions.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(true)
        .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
        .name("Shared Access Policy Primary Key")
        .description("The primary key of the Event Hub Shared Access Policy")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .sensitive(true)
        .required(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully sent to the Azure Event Hub will be transferred to this Relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that could not be sent to the Azure Event Hub will be transferred to this Relationship.")
        .build();

    private volatile BlockingQueue<EventHubSender> senderQueue = new LinkedBlockingQueue<>();


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(EVENT_HUB_NAME);
        properties.add(NAMESPACE);
        properties.add(ACCESS_POLICY);
        properties.add(POLICY_PRIMARY_KEY);
        return properties;
    }


    @OnScheduled
    public final void setupClient(final ProcessContext context) throws EventHubException {
        final String policyName = context.getProperty(ACCESS_POLICY).getValue();
        final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
        final String namespace = context.getProperty(NAMESPACE).getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();

        final EventHubClient client = EventHubClient.create(policyName, policyKey, namespace, eventHubName);

        final int numThreads = context.getMaxConcurrentTasks();
        senderQueue = new LinkedBlockingQueue<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final EventHubSender sender = client.createPartitionSender(null);
            senderQueue.offer(sender);
        }
    }

    @OnStopped
    public void tearDown() {
        EventHubSender sender;
        while ((sender = senderQueue.poll()) != null) {
            sender.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final EventHubSender sender = senderQueue.poll();
        try {
            final byte[] buffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, buffer);
                }
            });

            try {
                sender.send(buffer);
            } catch (final EventHubException ehe) {
                getLogger().error("Failed to send {} to EventHub due to {}; routing to failure", new Object[] { flowFile, ehe }, ehe);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            final String namespace = context.getProperty(NAMESPACE).getValue();
            final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
            session.getProvenanceReporter().send(flowFile, "amqps://" + namespace + ".servicebus.windows.net" + "/" + eventHubName, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } finally {
            senderQueue.offer(sender);
        }
    }
}
