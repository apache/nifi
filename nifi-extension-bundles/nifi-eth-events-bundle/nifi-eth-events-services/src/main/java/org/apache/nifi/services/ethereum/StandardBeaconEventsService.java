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
package org.apache.nifi.services.ethereum;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import io.github.amilcar.eth.eventslistener.client.BeaconEventClient;
import io.github.amilcar.eth.eventslistener.client.BeaconEventClientFactory;
import io.github.amilcar.eth.eventslistener.listener.BeaconEventListener;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Tags({"ethereum", "eth2", "beacon", "blockchain", "events", "controller", "service"})
@CapabilityDescription("ControllerService implementation for connecting to an Ethereum beacon chain node and listening for events. " +
        "This service allows processors to subscribe to specific event types and receive notifications when those events occur. " +
        "It uses the eth-events-listener library to connect to the beacon chain node and listen for events.")
public class StandardBeaconEventsService extends AbstractControllerService implements BeaconEventsService {

    private static final Logger logger = LoggerFactory.getLogger(StandardBeaconEventsService.class);

    public static final PropertyDescriptor BEACON_NODE_URL = new PropertyDescriptor
            .Builder()
            .name("Beacon Node URL")
            .displayName("Beacon Node URL")
            .description("URL of the Ethereum beacon chain node to connect to. This should be the HTTP endpoint of a running " +
                    "Ethereum beacon chain node that supports the standard beacon chain API. For example, " +
                    "http://localhost:5051 for a local node, or the URL of a public beacon chain node.")
            .required(true)
            .defaultValue("http://localhost:5051")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties = List.of(BEACON_NODE_URL);

    private BeaconEventClient beaconEventClient;
    private String beaconNodeUrl;

    // Map to store subscriptions: subscription ID -> consumer
    private final Map<String, Consumer<BeaconEvent>> subscriptions = new ConcurrentHashMap<>();

    // Map to track which event types each subscription is interested in
    private final Map<String, EnumSet<EventType>> subscriptionEventTypes = new ConcurrentHashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Initialize the service and connect to the beacon chain node.
     * This method is called when the service is enabled. It reads the configuration properties
     * and connects to the beacon chain node.
     * @param context the configuration context containing the service properties
     * @throws InitializationException if unable to connect to the beacon chain node
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        beaconNodeUrl = context.getProperty(BEACON_NODE_URL).getValue();

        try {
            logger.info("Connecting to beacon node at {}", beaconNodeUrl);

            // Create a client for all event types
            beaconEventClient = BeaconEventClientFactory.createClientForAllEvents(beaconNodeUrl);

            // We don't start the client here because we want to allow processors to subscribe
            // to specific event types first. The subscribeToEvents method will start the client.

            logger.info("Successfully created beacon event client");
        } catch (Exception e) {
            logger.error("Error creating beacon event client: {}", e.getMessage(), e);
            throw new InitializationException("Failed to create beacon event client: " + e.getMessage(), e);
        }
    }

    /**
     * Clean up resources when the service is disabled.
     * This method is called when the service is disabled. It clears all subscriptions
     * and closes the connection to the beacon chain node if one exists.
     */
    @OnDisabled
    public void shutdown() {
        logger.info("Shutting down beacon event client");

        // Clear all subscriptions
        subscriptions.clear();
        subscriptionEventTypes.clear();

        // Close the beacon event client if it exists
        if (beaconEventClient != null) {
            try {
                beaconEventClient.stop();
                beaconEventClient.close();
                logger.info("Beacon event client closed successfully");
            } catch (Exception e) {
                logger.error("Error closing beacon event client: {}", e.getMessage(), e);
            } finally {
                beaconEventClient = null;
            }
        }
    }

    /**
     * Legacy method, kept for backward compatibility
     */
    @Override
    public void execute() {
        // This method is kept for backward compatibility
        logger.debug("execute() method called, but it's deprecated. Use subscribeToEvents() instead.");
    }

    /**
     * Subscribe to beacon chain events of the specified types
     * @param eventTypes The types of events to subscribe to
     * @param eventConsumer The consumer that will process the events
     * @return A subscription ID that can be used to unsubscribe
     */
    @Override
    public String subscribeToEvents(EnumSet<EventType> eventTypes, Consumer<BeaconEvent> eventConsumer) {
        if (eventTypes == null || eventTypes.isEmpty()) {
            throw new IllegalArgumentException("Event types cannot be null or empty");
        }
        if (eventConsumer == null) {
            throw new IllegalArgumentException("Event consumer cannot be null");
        }

        String subscriptionId = UUID.randomUUID().toString();
        subscriptions.put(subscriptionId, eventConsumer);
        subscriptionEventTypes.put(subscriptionId, EnumSet.copyOf(eventTypes));

        // If the beacon event client is null, log an error and throw an exception
        if (beaconEventClient == null) {
            logger.error("Cannot subscribe to events: beacon event client is null. Service may not be properly initialized.");
            throw new IllegalStateException("Cannot subscribe to events: beacon event client is null");
        }

        try {
            // For each event type, add a listener to the client
            for (EventType eventType : eventTypes) {
                // Map internal EventType to external library enum
                final io.github.amilcar.eth.eventslistener.model.EventType externalType =
                        io.github.amilcar.eth.eventslistener.model.EventType.valueOf(eventType.name());

                // Create a listener that will dispatch converted events to the subscriber
                BeaconEventListener<io.github.amilcar.eth.eventslistener.model.BeaconEvent> eventListener = externalEvent -> {
                    logger.debug("Received external event: {}", externalEvent);
                    try {
                        // Convert external model to internal representation
                        final EventType internalType = EventType.valueOf(externalEvent.getEventType().name());
                        final String json = externalEvent.getData() != null ? externalEvent.getData().toString() : "{}";
                        final BeaconEvent internalEvent = new BeaconEvent(internalType, json);
                        eventConsumer.accept(internalEvent);
                    } catch (Exception e) {
                        logger.error("Error dispatching event to subscriber {}: {}", subscriptionId, e.getMessage(), e);
                    }
                };

                // Add the listener to the client
                beaconEventClient.addEventListener(externalType, eventListener);
            }

            // Start the client if it's not already started
            if (!beaconEventClient.isConnected()) {
                beaconEventClient.start();
            }

            logger.info("Created subscription {} for event types: {}", subscriptionId, eventTypes);
        } catch (Exception e) {
            logger.error("Error subscribing to events: {}", e.getMessage(), e);
            subscriptions.remove(subscriptionId);
            subscriptionEventTypes.remove(subscriptionId);
            throw new RuntimeException("Failed to subscribe to events: " + e.getMessage(), e);
        }

        return subscriptionId;
    }

    /**
     * Unsubscribe from events using the subscription ID
     * @param subscriptionId The subscription ID returned from subscribeToEvents
     * @return true if the subscription was found and removed, false otherwise
     */
    @Override
    public boolean unsubscribeFromEvents(String subscriptionId) {
        if (subscriptionId == null || subscriptionId.isEmpty()) {
            return false;
        }

    Consumer<BeaconEvent> removed = subscriptions.remove(subscriptionId);
    subscriptionEventTypes.remove(subscriptionId);

        if (removed != null) {
            // If we have a beacon event client and it's connected, remove the listeners
            if (beaconEventClient != null && beaconEventClient.isConnected()) {
                try {
                    // We can't remove specific listeners since we don't store them,
                    // but we can stop and restart the client if there are no more subscriptions
                    if (subscriptions.isEmpty()) {
                        beaconEventClient.stop();
                    }
                } catch (Exception e) {
                    logger.error("Error removing event listeners: {}", e.getMessage(), e);
                }
            }

            logger.info("Removed subscription {}", subscriptionId);
            return true;
        } else {
            logger.warn("Subscription {} not found", subscriptionId);
            return false;
        }
    }

    /**
     * Get the URL of the beacon chain node
     * @return The URL of the beacon chain node
     */
    @Override
    public String getBeaconNodeUrl() {
        return beaconNodeUrl;
    }
}
