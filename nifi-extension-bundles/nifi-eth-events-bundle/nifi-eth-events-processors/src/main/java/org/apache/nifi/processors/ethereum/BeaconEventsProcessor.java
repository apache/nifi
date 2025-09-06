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
package org.apache.nifi.processors.ethereum;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
// Validator replaced by method reference
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import org.apache.nifi.services.ethereum.BeaconEventsService;
import org.apache.nifi.services.ethereum.EventType;
import org.apache.nifi.services.ethereum.BeaconEvent;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Tags({ "ethereum", "eth2", "beacon", "blockchain", "web3j", "events" })
@CapabilityDescription("Listens for Ethereum beacon chain events and generates FlowFiles when events are received. " +
        "This processor connects to a controller service that provides access to Ethereum beacon chain events " +
        "and creates a FlowFile for each event received.")
@SeeAlso({})
@ReadsAttributes({})
@WritesAttributes({
        @WritesAttribute(attribute = "eth.event.type", description = "The type of Ethereum beacon chain event"),
        @WritesAttribute(attribute = "eth.event.timestamp", description = "The timestamp when the event was received"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME type of the event payload")
})
public class BeaconEventsProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ETH_EVENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Ethereum Events Service")
            .displayName("Ethereum Events Service")
            .description("The controller service that provides access to Ethereum beacon chain events")
            .required(true)
            .identifiesControllerService(BeaconEventsService.class)
            .build();

    public static final PropertyDescriptor EVENT_TYPES = new PropertyDescriptor.Builder()
            .name("Event Types")
            .displayName("Event Types")
            .description("Comma-separated list of event types to subscribe to. Valid values are: " +
                    java.util.Arrays.toString(EventType.values()).replace("[", "").replace("]", ""))
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(BeaconEventsProcessor::validateEventTypes)
            .build();



    private static ValidationResult validateEventTypes(final String subject, final String input,
            final ValidationContext context) {
        if (StringUtils.isBlank(input)) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Value is required and cannot be blank")
                    .build();
        }

        final String[] tokens = input.split(",");
        final StringBuilder invalids = new StringBuilder();
        for (final String raw : tokens) {
            final String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }
            try {
                EventType.valueOf(token);
            } catch (final IllegalArgumentException e) {
                if (invalids.length() > 0)
                    invalids.append(", ");
                invalids.append(token);
            }
        }

        if (invalids.length() > 0) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Invalid event type(s): " + invalids + ". Allowed: " +
                            java.util.Arrays.toString(EventType.values()))
                    .build();
        }

        return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(true)
                .build();
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing Ethereum beacon chain events")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // Store received events
    private final Map<String, BeaconEvent> receivedEvents = new ConcurrentHashMap<>();

    // Track subscription ID
    private final AtomicReference<String> subscriptionId = new AtomicReference<>();

    // Track reconfiguration requests triggered by property changes
    private final AtomicBoolean reconfigureRequested = new AtomicBoolean(false);

    // Guard to ignore events from stale subscriptions
    private final AtomicLong subscriptionGeneration = new AtomicLong(0);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(ETH_EVENT_SERVICE, EVENT_TYPES);
        relationships = Set.of(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Initial subscription when scheduled
        subscribeOrResubscribe(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        String subId = subscriptionId.getAndSet(null);
        if (subId != null) {
            final BeaconEventsService service = context.getProperty(ETH_EVENT_SERVICE)
                    .asControllerService(BeaconEventsService.class);
            boolean unsubscribed = service.unsubscribeFromEvents(subId);
            if (unsubscribed) {
                getLogger().info("Unsubscribed from Ethereum events with subscription ID: {}", subId);
            } else {
                getLogger().warn("Failed to unsubscribe from Ethereum events with subscription ID: {}", subId);
            }
        }

        // Clear any pending events
        receivedEvents.clear();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // If the controller service or the event types changed, request a
        // resubscription.
        if (ETH_EVENT_SERVICE.equals(descriptor) || EVENT_TYPES.equals(descriptor)) {
            reconfigureRequested.set(true);
            getLogger().debug("Property '{}' changed. Will reconfigure subscription on next trigger.",
                    descriptor.getDisplayName());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        // If a reconfiguration was requested while running, perform it now.
        if (reconfigureRequested.compareAndSet(true, false)) {
            // Best-effort resubscribe with current context
            try {
                unsubscribeCurrent(context);
                subscribeOrResubscribe(context);
            } catch (final Exception e) {
                getLogger().warn("Failed to reconfigure subscription: {}", e.getMessage(), e);
                // Avoid processing potentially mismatched events
                context.yield();
                return;
            }
        }

        // Process any received events
        if (receivedEvents.isEmpty()) {
            context.yield(); // No events to process, yield to other processors
            return;
        }

        // Get a copy of the events and clear the original map to avoid processing the
        // same events multiple times
        Map<String, BeaconEvent> eventsToProcess = new HashMap<>(receivedEvents);
        receivedEvents.keySet().removeAll(eventsToProcess.keySet());

        for (Map.Entry<String, BeaconEvent> entry : eventsToProcess.entrySet()) {
            BeaconEvent event = entry.getValue();

            // Create a FlowFile for the event
            FlowFile flowFile = session.create();

            try {
                // Write the event data to the FlowFile
                flowFile = session.write(flowFile, (out) -> {
                    writeEventToOutputStream(event, out);
                });

                // Add attributes
                Map<String, String> attributes = new HashMap<>();
                attributes.put("eth.event.type", event.getEventType().name());
                attributes.put("eth.event.timestamp", String.valueOf(System.currentTimeMillis()));
                attributes.put("mime.type", "application/json");

                flowFile = session.putAllAttributes(flowFile, attributes);

                // Transfer the FlowFile to the success relationship
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, context.getProperty(ETH_EVENT_SERVICE)
                        .asControllerService(BeaconEventsService.class).getBeaconNodeUrl());

                getLogger().debug("Generated FlowFile for event: {}", event);
            } catch (Exception e) {
                getLogger().error("Failed to process event: {}", event, e);
                session.remove(flowFile);
            }
        }
    }

    private void subscribeOrResubscribe(final ProcessContext context) {
        final BeaconEventsService service = context.getProperty(ETH_EVENT_SERVICE)
                .asControllerService(BeaconEventsService.class);
        if (service == null) {
            getLogger().warn("Ethereum Events Service is not configured or not enabled.");
            return;
        }

        final String eventTypesStr = context.getProperty(EVENT_TYPES).getValue();

        // Parse event types
        EnumSet<EventType> eventTypes = EnumSet.noneOf(EventType.class);
        if (!StringUtils.isBlank(eventTypesStr)) {
            for (String eventType : eventTypesStr.split(",")) {
                final String t = eventType.trim();
                if (!t.isEmpty()) {
                    try {
                        eventTypes.add(EventType.valueOf(t));
                    } catch (IllegalArgumentException e) {
                        getLogger().warn("Invalid event type: {}", t);
                    }
                }
            }
        }

        if (eventTypes.isEmpty()) {
            getLogger().warn("No valid event types specified, processor will not receive any events");
            return;
        }

        // New generation to invalidate any previous listeners that may still be
        // attached in the service
        final long generation = subscriptionGeneration.incrementAndGet();

        // Subscribe to events
        String subId = service.subscribeToEvents(eventTypes, event -> {
            // Guard to ignore events from stale subscriptions
            if (generation != subscriptionGeneration.get()) {
                return;
            }
            // Store the event for processing in onTrigger
            String eventId = UUID.randomUUID().toString();
            receivedEvents.put(eventId, event);
            getLogger().debug("Received event: {}", event);
        });

        subscriptionId.set(subId);
        getLogger().info("Subscribed to Ethereum events with subscription ID: {}", subId);
    }

    private void unsubscribeCurrent(final ProcessContext context) {
        final String subId = subscriptionId.getAndSet(null);
        if (subId == null) {
            return;
        }
        final BeaconEventsService service = context.getProperty(ETH_EVENT_SERVICE)
                .asControllerService(BeaconEventsService.class);
        if (service == null) {
            return;
        }
        boolean ok = false;
        try {
            ok = service.unsubscribeFromEvents(subId);
        } catch (final Exception e) {
            getLogger().warn("Error unsubscribing from previous subscription {}: {}", subId, e.getMessage(), e);
        }
        if (ok) {
            getLogger().info("Unsubscribed from previous subscription {}", subId);
        } else {
            getLogger().warn("Failed to unsubscribe from previous subscription {}", subId);
        }
        // Clear pending events to avoid mixing different configurations
        receivedEvents.clear();
    }

    private void writeEventToOutputStream(final BeaconEvent event, final OutputStream out)
            throws IOException {
        // JSON serialization
        final StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"type\":\"").append(event.getEventType()).append("\",");
        // event data is already JSON string; embed as raw JSON field named `data`
        json.append("\"data\":").append(event.getJsonData());
        json.append("}");
        out.write(json.toString().getBytes(StandardCharsets.UTF_8));
    }

}
