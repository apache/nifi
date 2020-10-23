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
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;

@Tags({"azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams"})
@CapabilityDescription("Receives messages from Microsoft Azure Event Hubs, writing the contents of the Azure message to the content of the FlowFile. "
        + "Note: Please be aware that this processor creates a thread pool of 4 threads for Event Hub Client. They will be extra threads other than the concurrent tasks scheduled for this processor.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the event hub"),
        @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
        @WritesAttribute(attribute = "eventhub.sequence", description = "The Azure sequence number associated with the message"),
        @WritesAttribute(attribute = "eventhub.name", description = "The name of the event hub from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.partition", description = "The name of the event hub partition from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.property.*", description = "The application properties of this message. IE: 'application' would be 'eventhub.property.application'")
})
public class GetAzureEventHub extends AbstractProcessor {
    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("The name of the event hub to pull messages from")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Event Hub Namespace")
            .description("The namespace that the event hub is assigned to. This is generally equal to <Event Hubs Name>-ns")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor SERVICE_BUS_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Service Bus Endpoint")
            .description("To support namespaces in non-standard Host URIs ( not .servicebus.windows.net,  ie .servicebus.chinacloudapi.cn) select from the drop down acceptable options ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(".servicebus.windows.net",".servicebus.chinacloudapi.cn")
            .defaultValue(".servicebus.windows.net")
            .required(true)
            .build();
    static final PropertyDescriptor ACCESS_POLICY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Name")
            .description("The name of the shared access policy. This policy must have Listen claims.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY =  AzureEventHubUtils.POLICY_PRIMARY_KEY;
    static final PropertyDescriptor USE_MANAGED_IDENTITY = AzureEventHubUtils.USE_MANAGED_IDENTITY;

    static final PropertyDescriptor NUM_PARTITIONS = new PropertyDescriptor.Builder()
            .name("Number of Event Hub Partitions")
            .description("The number of partitions that the event hub has. Only this number of partitions will be used, "
                    + "so it is important to ensure that if the number of partitions changes that this value be updated. Otherwise, some messages may not be consumed.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
            .name("Event Hub Consumer Group")
            .displayName("Consumer Group")
            .description("The name of the consumer group to use when pulling events")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("$Default")
            .required(true)
            .build();

    static final PropertyDescriptor ENQUEUE_TIME = new PropertyDescriptor.Builder()
            .name("Event Hub Message Enqueue Time")
            .displayName("Message Enqueue Time")
            .description("A timestamp (ISO-8601 Instant) formatted as YYYY-MM-DDThhmmss.sssZ (2016-01-01T01:01:01.000Z) from which messages "
                    + "should have been enqueued in the Event Hub to start reading from")
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECEIVER_FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Partition Recivier Fetch Size")
            .displayName("Partition Receiver Fetch Size")
            .description("The number of events that a receiver should fetch from an Event Hubs partition before returning. Default(100)")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECEIVER_FETCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Partiton Receiver Timeout (millseconds)")
            .name("Partition Receiver Timeout (millseconds)")
            .description("The amount of time a Partition Receiver should wait to receive the Fetch Size before returning. Default(60000)")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully received from the event hub will be transferred to this Relationship.")
            .build();


    private final ConcurrentMap<String, PartitionReceiver> partitionToReceiverMap = new ConcurrentHashMap<>();
    private volatile BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();
    private volatile Instant configuredEnqueueTime;
    private volatile int receiverFetchSize;
    private volatile Duration receiverFetchTimeout;
    private EventHubClient eventHubClient;

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(EVENT_HUB_NAME);
        _propertyDescriptors.add(SERVICE_BUS_ENDPOINT);
        _propertyDescriptors.add(NAMESPACE);
        _propertyDescriptors.add(ACCESS_POLICY);
        _propertyDescriptors.add(POLICY_PRIMARY_KEY);
        _propertyDescriptors.add(USE_MANAGED_IDENTITY);
        _propertyDescriptors.add(NUM_PARTITIONS);
        _propertyDescriptors.add(CONSUMER_GROUP);
        _propertyDescriptors.add(ENQUEUE_TIME);
        _propertyDescriptors.add(RECEIVER_FETCH_SIZE);
        _propertyDescriptors.add(RECEIVER_FETCH_TIMEOUT);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> retVal = AzureEventHubUtils.customValidate(ACCESS_POLICY, POLICY_PRIMARY_KEY, context);
        return retVal;
    }

    protected void setupReceiver(final String connectionString, final ScheduledExecutorService executor) throws ProcessException {
        try {
            EventHubClientImpl.USER_AGENT = "ApacheNiFi-azureeventhub/3.1.1";
            eventHubClient = EventHubClient.createFromConnectionStringSync(connectionString, executor);
        } catch (IOException | EventHubException e) {
            throw new ProcessException(e);
        }
    }

    PartitionReceiver getReceiver(final ProcessContext context, final String partitionId) throws IOException, EventHubException, ExecutionException, InterruptedException {
        PartitionReceiver existingReceiver = partitionToReceiverMap.get(partitionId);
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

            final String consumerGroupName = context.getProperty(CONSUMER_GROUP).getValue();

            final PartitionReceiver receiver = eventHubClient.createReceiver(
                    consumerGroupName,
                    partitionId,
                    EventPosition.fromEnqueuedTime(
                            configuredEnqueueTime == null ? Instant.now() : configuredEnqueueTime)).get();

            receiver.setReceiveTimeout(receiverFetchTimeout == null ? Duration.ofMillis(60000) : receiverFetchTimeout);
            partitionToReceiverMap.put(partitionId, receiver);
            return receiver;

        }
    }

    /**
     * This method is here to try and isolate the Azure related code as the PartitionReceiver cannot be mocked
     * with PowerMock due to it being final. Unfortunately it extends a base class and does not implement an interface
     * so even if we create a MockPartitionReciver, it will not work as the two classes are orthogonal.
     *
     * @param context     - The processcontext for this processor
     * @param partitionId - The partition ID to retrieve a receiver by.
     * @return - Returns the events received from the EventBus.
     * @throws ProcessException -- If any exception is encountered, receiving events it is wrapped in a ProcessException
     *                          and then that exception is thrown.
     */
    protected Iterable<EventData> receiveEvents(final ProcessContext context, final String partitionId) throws ProcessException {
        final PartitionReceiver receiver;
        try {
            receiver = getReceiver(context, partitionId);
            return receiver.receive(receiverFetchSize).get();
        } catch (final EventHubException | IOException | ExecutionException | InterruptedException e) {
            throw new ProcessException(e);
        }
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        for (final PartitionReceiver receiver : partitionToReceiverMap.values()) {
            if (null != receiver) {
                receiver.close();
            }
        }

        partitionToReceiverMap.clear();
        try {
            if (null != eventHubClient) {
                eventHubClient.closeSync();
            }
            executor.shutdown();
        } catch (final EventHubException e) {
            throw new ProcessException(e);
        }
    }

    private ScheduledExecutorService executor;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException, URISyntaxException {
        final BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();
        for (int i = 0; i < context.getProperty(NUM_PARTITIONS).asInteger(); i++) {
            partitionNames.add(String.valueOf(i));
        }
        this.partitionNames = partitionNames;

        final String namespace = context.getProperty(NAMESPACE).getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();
        final String connectionString;

        if(useManagedIdentity){
            connectionString = AzureEventHubUtils.getManagedIdentityConnectionString(namespace,eventHubName);
        } else {
            final String policyName = context.getProperty(ACCESS_POLICY).getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            connectionString = new ConnectionStringBuilder()
                                    .setEndpoint(new URI("amqps://"+namespace+serviceBusEndpoint))
                                    .setEventHubName(eventHubName)
                                    .setSasKeyName(policyName)
                                    .setSasKey(policyKey).toString();
        }

        if(context.getProperty(ENQUEUE_TIME).isSet()) {
            configuredEnqueueTime = Instant.parse(context.getProperty(ENQUEUE_TIME).toString());
        } else {
            configuredEnqueueTime = null;
        }
        if(context.getProperty(RECEIVER_FETCH_SIZE).isSet()) {
            receiverFetchSize = context.getProperty(RECEIVER_FETCH_SIZE).asInteger();
        } else {
            receiverFetchSize = 100;
        }
        if(context.getProperty(RECEIVER_FETCH_TIMEOUT).isSet()) {
            receiverFetchTimeout = Duration.ofMillis(context.getProperty(RECEIVER_FETCH_TIMEOUT).asLong());
        } else {
            receiverFetchTimeout = null;
        }

        executor = Executors.newScheduledThreadPool(4);
        setupReceiver(connectionString, executor);
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

            final Iterable<EventData> receivedEvents = receiveEvents(context, partitionId);
            if (receivedEvents == null) {
                return;
            }

            for (final EventData eventData : receivedEvents) {
                if (null != eventData) {

                    final Map<String, String> attributes = new HashMap<>();
                    FlowFile flowFile = session.create();
                    final EventData.SystemProperties systemProperties = eventData.getSystemProperties();

                    if (null != systemProperties) {
                        attributes.put("eventhub.enqueued.timestamp", String.valueOf(systemProperties.getEnqueuedTime()));
                        attributes.put("eventhub.offset", systemProperties.getOffset());
                        attributes.put("eventhub.sequence", String.valueOf(systemProperties.getSequenceNumber()));
                    }

                    final Map<String,String> applicationProperties = AzureEventHubUtils.getApplicationProperties(eventData);
                    attributes.putAll(applicationProperties);

                    attributes.put("eventhub.name", context.getProperty(EVENT_HUB_NAME).getValue());
                    attributes.put("eventhub.partition", partitionId);


                    flowFile = session.putAllAttributes(flowFile, attributes);
                    flowFile = session.write(flowFile, out -> {
                        out.write(eventData.getBytes());
                    });

                    session.transfer(flowFile, REL_SUCCESS);

                    final String namespace = context.getProperty(NAMESPACE).getValue();
                    final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
                    final String consumerGroup = context.getProperty(CONSUMER_GROUP).getValue();
                    final String serviceBusEndPoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
                    final String transitUri = "amqps://" + namespace + serviceBusEndPoint + "/" + eventHubName + "/ConsumerGroups/" + consumerGroup + "/Partitions/" + partitionId;
                    session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                }
            }
        } finally {
            partitionIds.offer(partitionId);
        }
    }
}
