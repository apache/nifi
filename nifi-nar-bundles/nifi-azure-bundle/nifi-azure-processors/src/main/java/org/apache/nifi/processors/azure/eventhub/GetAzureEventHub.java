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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import com.microsoft.eventhubs.client.ConnectionStringBuilder;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubMessage;
import com.microsoft.eventhubs.client.IEventHubFilter;
import com.microsoft.eventhubs.client.ResilientEventHubReceiver;

@Tags({ "azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams" })
@CapabilityDescription("Receives messages from a Microsoft Azure Event Hub, writing the contents of the Azure message to the content of the FlowFile")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the Azure Event Hub"),
    @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
    @WritesAttribute(attribute = "eventhub.sequence", description = "The Azure Sequence number associated with the message"),
    @WritesAttribute(attribute = "eventhub.name", description = "The name of the Event Hub from which the message was pulled"),
    @WritesAttribute(attribute = "eventhub.partition", description = "The name of the Azure Partition from which the message was pulled")
})
public class GetAzureEventHub extends AbstractProcessor {

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
        .name("Event Hub Name")
        .description("The name of the Azure Event Hub to pull messages from")
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
        .description("The name of the Event Hub Shared Access Policy. This Policy must have Listen permissions.")
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

    static final PropertyDescriptor NUM_PARTITIONS = new PropertyDescriptor.Builder()
        .name("Number of Event Hub Partitions")
        .description("The number of partitions that the Event Hub has. Only this number of partitions will be used, "
            + "so it is important to ensure that if the number of partitions changes that this value be updated. Otherwise, some messages may not be consumed.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(true)
        .build();
    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
        .name("Event Hub Consumer Group")
        .description("The name of the Event Hub Consumer Group to use when pulling events")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("$Default")
        .required(true)
        .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The number of FlowFiles to pull in a single JMS session")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("10")
        .required(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully received from the Azure Event Hub will be transferred to this Relationship.")
        .build();

    private final ConcurrentMap<String, ResilientEventHubReceiver> partitionToReceiverMap = new ConcurrentHashMap<>();
    private volatile BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(EVENT_HUB_NAME);
        properties.add(NAMESPACE);
        properties.add(ACCESS_POLICY);
        properties.add(POLICY_PRIMARY_KEY);
        properties.add(NUM_PARTITIONS);
        properties.add(CONSUMER_GROUP);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    private ResilientEventHubReceiver getReceiver(final ProcessContext context, final String partitionId) throws EventHubException {
        ResilientEventHubReceiver existingReceiver = partitionToReceiverMap.get(partitionId);
        if (existingReceiver != null) {
            return existingReceiver;
        }

        // we want to avoid allowing multiple threads to create Receivers simultaneously because that could result in
        // having multiple Receivers for the same partition. So if the map does not contain a receiver for this partition,
        // we will enter a synchronized block and check again (because once we enter the synchronized block, we know that no
        // other thread is creating a client). If within the synchronized block, we still do not have an entry in the map,
        // it is up to use to create the receiver, initialize it, and then put it into the map.
        // We do not use the putIfAbsent method in order to do a CAS operation here because we want to also initialize the
        // receiver if and only if it is not present in the map. As a result, we need to initialize the receiver and add it
        // to the map atomically. Hence, the synchronized block.
        synchronized (this) {
            existingReceiver = partitionToReceiverMap.get(partitionId);
            if (existingReceiver != null) {
                return existingReceiver;
            }

            final String policyName = context.getProperty(ACCESS_POLICY).getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            final String namespace = context.getProperty(NAMESPACE).getValue();
            final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
            final String consumerGroupName = context.getProperty(CONSUMER_GROUP).getValue();

            final String connectionString = new ConnectionStringBuilder(policyName, policyKey, namespace).getConnectionString();
            final IEventHubFilter filter = new EventHubEnqueueTimeFilter(System.currentTimeMillis());
            final ResilientEventHubReceiver receiver = new ResilientEventHubReceiver(connectionString, eventHubName, partitionId, consumerGroupName, -1, filter);
            receiver.initialize();

            partitionToReceiverMap.put(partitionId, receiver);
            return receiver;
        }
    }


    @OnStopped
    public void tearDown() {
        for (final ResilientEventHubReceiver receiver : partitionToReceiverMap.values()) {
            receiver.close();
        }

        partitionToReceiverMap.clear();
    }

    @OnScheduled
    public void setupPartitions(final ProcessContext context) {
        final BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();
        for (int i = 0; i < context.getProperty(NUM_PARTITIONS).asInteger(); i++) {
            partitionNames.add(String.valueOf(i));
        }
        this.partitionNames = partitionNames;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final BlockingQueue<String> partitionIds = this.partitionNames;
        final String partitionId = partitionIds.poll();
        if (partitionId == null) {
            getLogger().debug("No partitions available");
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            final ResilientEventHubReceiver receiver;
            try {
                receiver = getReceiver(context, partitionId);
            } catch (final EventHubException e) {
                throw new ProcessException(e);
            }

            final EventHubMessage message = EventHubMessage.parseAmqpMessage(receiver.receive(100L));
            if (message == null) {
                return;
            }

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("eventhub.enqueued.timestamp", String.valueOf(message.getEnqueuedTimestamp()));
            attributes.put("eventhub.offset", message.getOffset());
            attributes.put("eventhub.sequence", String.valueOf(message.getSequence()));
            attributes.put("eventhub.name", context.getProperty(EVENT_HUB_NAME).getValue());
            attributes.put("eventhub.partition", partitionId);

            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(message.getData());
                }
            });

            session.transfer(flowFile, REL_SUCCESS);

            final String namespace = context.getProperty(NAMESPACE).getValue();
            final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
            final String consumerGroup = context.getProperty(CONSUMER_GROUP).getValue();
            final String transitUri = "amqps://" + namespace + ".servicebus.windows.net" + "/" + eventHubName + "/ConsumerGroups/" + consumerGroup + "/Partitions/" + partitionId;
            session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        } finally {
            partitionIds.offer(partitionId);
        }
    }

}
