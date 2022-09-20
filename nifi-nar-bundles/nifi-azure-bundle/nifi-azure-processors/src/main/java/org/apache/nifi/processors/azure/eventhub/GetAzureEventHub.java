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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.PartitionProperties;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionEvent;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
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
    private static final String TRANSIT_URI_FORMAT_STRING = "amqps://%s/%s/ConsumerGroups/%s/Partitions/%s";

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("Name of Azure Event Hubs source")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Event Hub Namespace")
            .description("Namespace of Azure Event Hubs prefixed to Service Bus Endpoint domain")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor SERVICE_BUS_ENDPOINT =AzureEventHubUtils.SERVICE_BUS_ENDPOINT;
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
            .description("The number of events that a receiver should fetch from an Event Hubs partition before returning. The default is 100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECEIVER_FETCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Partiton Receiver Timeout (millseconds)")
            .name("Partition Receiver Timeout (millseconds)")
            .displayName("Partition Receiver Timeout")
            .description("The amount of time in milliseconds a Partition Receiver should wait to receive the Fetch Size before returning. The default is 60000")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully received from the event hub will be transferred to this Relationship.")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    static {
        propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
                EVENT_HUB_NAME,
                SERVICE_BUS_ENDPOINT,
                NAMESPACE,
                ACCESS_POLICY,
                POLICY_PRIMARY_KEY,
                USE_MANAGED_IDENTITY,
                NUM_PARTITIONS,
                CONSUMER_GROUP,
                ENQUEUE_TIME,
                RECEIVER_FETCH_SIZE,
                RECEIVER_FETCH_TIMEOUT
        ));
        relationships = Collections.singleton(REL_SUCCESS);
    }

    private volatile BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();
    private volatile Instant initialEnqueuedTime;
    private volatile EventPosition initialEventPosition;
    private volatile int receiverFetchSize;
    private volatile Duration receiverFetchTimeout;

    private EventHubConsumerClient eventHubConsumerClient;

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
        return AzureEventHubUtils.customValidate(ACCESS_POLICY, POLICY_PRIMARY_KEY, context);
    }

    @OnStopped
    public void closeClient() {
        if (eventHubConsumerClient == null) {
            getLogger().info("Azure Event Hub Consumer Client not configured");
        } else {
            eventHubConsumerClient.close();
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BlockingQueue<String> partitionNames = new LinkedBlockingQueue<>();
        for (int i = 0; i < context.getProperty(NUM_PARTITIONS).asInteger(); i++) {
            partitionNames.add(String.valueOf(i));
        }
        this.partitionNames = partitionNames;

        final String namespace = context.getProperty(NAMESPACE).getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();
        final String fullyQualifiedNamespace = String.format("%s%s", namespace, serviceBusEndpoint);

        final EventHubClientBuilder eventHubClientBuilder = new EventHubClientBuilder();

        final String consumerGroup = context.getProperty(CONSUMER_GROUP).getValue();
        eventHubClientBuilder.consumerGroup(consumerGroup);

        if (useManagedIdentity) {
            final ManagedIdentityCredentialBuilder managedIdentityCredentialBuilder = new ManagedIdentityCredentialBuilder();
            final ManagedIdentityCredential managedIdentityCredential = managedIdentityCredentialBuilder.build();
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, managedIdentityCredential);
        } else {
            final String policyName = context.getProperty(ACCESS_POLICY).getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            final AzureNamedKeyCredential azureNamedKeyCredential = new AzureNamedKeyCredential(policyName, policyKey);
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, azureNamedKeyCredential);
        }
        eventHubConsumerClient = eventHubClientBuilder.buildConsumerClient();

        final PropertyValue enqueuedTimeProperty = context.getProperty(ENQUEUE_TIME);
        if (enqueuedTimeProperty.isSet()) {
            initialEnqueuedTime = Instant.parse(enqueuedTimeProperty.getValue());
        } else {
            initialEnqueuedTime = Instant.now();
        }

        if (context.getProperty(RECEIVER_FETCH_SIZE).isSet()) {
            receiverFetchSize = context.getProperty(RECEIVER_FETCH_SIZE).asInteger();
        } else {
            receiverFetchSize = 100;
        }
        if (context.getProperty(RECEIVER_FETCH_TIMEOUT).isSet()) {
            receiverFetchTimeout = Duration.ofMillis(context.getProperty(RECEIVER_FETCH_TIMEOUT).asLong());
        } else {
            receiverFetchTimeout = Duration.ofMillis(60000);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String partitionId = partitionNames.poll();
        if (partitionId == null) {
            getLogger().debug("No partitions available");
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            final Iterable<PartitionEvent> events = receiveEvents(partitionId);
            for (final PartitionEvent partitionEvent : events) {
                final Map<String, String> attributes = getAttributes(partitionEvent);

                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);

                final EventData eventData = partitionEvent.getData();
                final byte[] body = eventData.getBody();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(body));

                session.transfer(flowFile, REL_SUCCESS);

                final String transitUri = getTransitUri(partitionId);
                session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            }
        } finally {
            partitionNames.offer(partitionId);
        }
    }

    /**
     * Receive Events from specified partition is synchronized to avoid concurrent requests for the same partition
     *
     * @param partitionId Partition Identifier
     * @return Iterable of Partition Events or empty when none received
     */
    protected synchronized Iterable<PartitionEvent> receiveEvents(final String partitionId) {
        final EventPosition eventPosition;

        if (initialEventPosition == null) {
            getLogger().debug("Receiving Events for Partition [{}] from Initial Enqueued Time [{}]", partitionId, initialEnqueuedTime);
            initialEventPosition = EventPosition.fromEnqueuedTime(initialEnqueuedTime);
            eventPosition = initialEventPosition;
        } else {
            final PartitionProperties partitionProperties = eventHubConsumerClient.getPartitionProperties(partitionId);
            final Instant lastEnqueuedTime = partitionProperties.getLastEnqueuedTime();
            getLogger().debug("Receiving Events for Partition [{}] from Last Enqueued Time [{}]", partitionId, lastEnqueuedTime);
            eventPosition = EventPosition.fromEnqueuedTime(lastEnqueuedTime);
        }

        return eventHubConsumerClient.receiveFromPartition(partitionId, receiverFetchSize, eventPosition, receiverFetchTimeout);
    }

    private String getTransitUri(final String partitionId) {
        return String.format(TRANSIT_URI_FORMAT_STRING,
                eventHubConsumerClient.getFullyQualifiedNamespace(),
                eventHubConsumerClient.getEventHubName(),
                eventHubConsumerClient.getConsumerGroup(),
                partitionId
        );
    }

    private Map<String, String> getAttributes(final PartitionEvent partitionEvent) {
        final Map<String, String> attributes = new LinkedHashMap<>();

        final LastEnqueuedEventProperties lastEnqueuedEventProperties = partitionEvent.getLastEnqueuedEventProperties();
        if (lastEnqueuedEventProperties != null) {
            attributes.put("eventhub.enqueued.timestamp", String.valueOf(lastEnqueuedEventProperties.getEnqueuedTime()));
            attributes.put("eventhub.offset", lastEnqueuedEventProperties.getOffset().toString());
            attributes.put("eventhub.sequence", String.valueOf(lastEnqueuedEventProperties.getSequenceNumber()));
        }

        final PartitionContext partitionContext = partitionEvent.getPartitionContext();
        attributes.put("eventhub.name", partitionContext.getEventHubName());
        attributes.put("eventhub.partition", partitionContext.getPartitionId());

        final EventData eventData = partitionEvent.getData();
        final Map<String,String> applicationProperties = AzureEventHubUtils.getApplicationProperties(eventData.getProperties());
        attributes.putAll(applicationProperties);

        return attributes;
    }
}
