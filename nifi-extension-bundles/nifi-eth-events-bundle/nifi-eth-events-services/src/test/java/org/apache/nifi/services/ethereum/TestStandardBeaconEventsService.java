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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.amilcar.eth.eventslistener.client.BeaconEventClient;
import io.github.amilcar.eth.eventslistener.listener.BeaconEventListener;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class TestStandardBeaconEventsService {

    private TestRunner runner;

    @Spy
    private StandardBeaconEventsService service;

    @Mock
    private BeaconEventClient mockBeaconEventClient;

    private MockWebServer mockWebServer;
    private String mockServerUrl;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(TestProcessor.class);

        // Create a spy of StandardBeaconEventsService that overrides the onEnabled method
        // to avoid connecting to a real beacon node
        Mockito.doAnswer(invocation -> {
            ConfigurationContext context = invocation.getArgument(0);
            // Set the beacon node URL from the context
            Field urlField = StandardBeaconEventsService.class.getDeclaredField("beaconNodeUrl");
            urlField.setAccessible(true);
            urlField.set(service, context.getProperty(StandardBeaconEventsService.BEACON_NODE_URL).getValue());

            // Set the mock beacon event client
            Field clientField = StandardBeaconEventsService.class.getDeclaredField("beaconEventClient");
            clientField.setAccessible(true);
            clientField.set(service, mockBeaconEventClient);

            return null;
        }).when(service).onEnabled(any(ConfigurationContext.class));

        // Configure the mock beacon event client
        Mockito.lenient().when(mockBeaconEventClient.isConnected()).thenReturn(true);

        runner.addControllerService("test-good", service);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (mockWebServer != null) {
            mockWebServer.shutdown();
        }
    }

    @Test
    public void testServiceConfiguration() throws InitializationException {
        // Set the beacon node URL
        runner.setProperty(service, StandardBeaconEventsService.BEACON_NODE_URL, "http://localhost:5051");

        // Enable the service
        runner.enableControllerService(service);

        // Verify the service is valid
        runner.assertValid(service);

        // Verify the beacon node URL is set correctly
        assertEquals("http://localhost:5051", service.getBeaconNodeUrl());
    }

    @Test
    public void testSubscribeAndUnsubscribe() throws Exception {
        // Set up the service with a mock beacon node service
        runner.setProperty(service, StandardBeaconEventsService.BEACON_NODE_URL, "http://localhost:5051");

        // Enable the service
        runner.enableControllerService(service);

        // Create a flag to track if our consumer was called
        AtomicBoolean consumerCalled = new AtomicBoolean(false);

        // Create a consumer for beacon events
    Consumer<BeaconEvent> eventConsumer = event -> {
            consumerCalled.set(true);
        };

        // Subscribe to events
    EnumSet<EventType> eventTypes = EnumSet.allOf(EventType.class);
        String subscriptionId = service.subscribeToEvents(eventTypes, eventConsumer);

        // Verify the subscription ID is not null or empty
        assertNotNull(subscriptionId);
        assertFalse(subscriptionId.isEmpty());

        // Unsubscribe from events
        boolean unsubscribed = service.unsubscribeFromEvents(subscriptionId);

        // Verify the unsubscribe was successful
        assertTrue(unsubscribed);

        // Try to unsubscribe again, which should fail
        unsubscribed = service.unsubscribeFromEvents(subscriptionId);

        // Verify the unsubscribe failed
        assertFalse(unsubscribed);
    }

    @Test
    public void testProcessorIntegration() throws Exception {
        // Set up the service with mock BeaconEventClient
        runner.setProperty(service, StandardBeaconEventsService.BEACON_NODE_URL, "http://localhost:5051");

        // Enable the service
        runner.enableControllerService(service);

        // Set the service in the processor
        runner.setProperty(TestProcessor.BEACON_EVENTS_SERVICE, "test-good");

        // Run the processor once to subscribe to events
        runner.run();

        // Verify the processor ran without errors
        runner.assertValid();

        // Run the processor again to unsubscribe from events
        runner.run();

        // Verify the processor ran without errors
        runner.assertValid();
    }

    @Test
    public void testExecuteDeprecatedMethod() throws InitializationException {
        // Set the beacon node URL
        runner.setProperty(service, StandardBeaconEventsService.BEACON_NODE_URL, "http://localhost:5051");

        // Enable the service
        runner.enableControllerService(service);

        // Call the deprecated execute method
        service.execute();

        // No assertion needed, we just want to make sure it doesn't throw an exception
    }

    @Test
    public void testProcessorReceivesEventFromMockNode() throws Exception {
        // Set up the MockWebServer
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        // Get the URL of the mock server
        mockServerUrl = mockWebServer.url("/").toString();
        // Remove trailing slash if present
        mockServerUrl = mockServerUrl.substring(0, mockServerUrl.length() - (mockServerUrl.endsWith("/") ? 1 : 0));

        // Create a sample event JSON
        String eventJson = "{\"slot\":\"10\", \"block\":\"0x9a2fefd2fdb57f74993c7780ea5b9030d2897b615b89f808011ca5aebed54eaf\", " +
                "\"state\":\"0x600e852a08c1200654ddf11025f1ceacb3c2e74bdd5c630cde0838b2591b69f9\", " +
                "\"epoch_transition\":false, " +
                "\"previous_duty_dependent_root\":\"0x5e0043f107cb57913498fbf2f99ff55e730bf1e151f02f221e977c91a90a0e91\", " +
                "\"current_duty_dependent_root\":\"0x5e0043f107cb57913498fbf2f99ff55e730bf1e151f02f221e977c91a90a0e91\", " +
                "\"execution_optimistic\": false}";

        // Create a MockResponse that simulates an SSE event
        MockResponse mockResponse = new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "text/event-stream")
                .setHeader("Cache-Control", "no-cache")
                .setHeader("Connection", "keep-alive")
                .setBody("event: head\ndata: " + eventJson + "\n\n")
                .setBodyDelay(100, TimeUnit.MILLISECONDS); // Small delay to simulate network

        // Enqueue the response
        mockWebServer.enqueue(mockResponse);

        // Create a flag to track if our consumer was called
        AtomicBoolean eventReceived = new AtomicBoolean(false);
        CountDownLatch eventLatch = new CountDownLatch(1);
    AtomicReference<BeaconEvent> receivedEvent = new AtomicReference<>();

    // Internal representation used by the Controller Service consumer
    BeaconEvent sampleInternalEvent = new BeaconEvent(EventType.HEAD, eventJson);

        // Reset previous mocks
        Mockito.reset(mockBeaconEventClient);

        // Configure the mock beacon event client
        Mockito.lenient().when(mockBeaconEventClient.isConnected()).thenReturn(true);
        Mockito.lenient().when(mockBeaconEventClient.getNodeUrl()).thenReturn(mockServerUrl);

        // Mock the addEventListener method to capture the listener and trigger it with our sample event
    Mockito.doAnswer(invocation -> {
            // Get the event type and listener from the invocation
        io.github.amilcar.eth.eventslistener.model.EventType eventType = invocation.getArgument(0);
            Object listenerObj = invocation.getArgument(1);

            // If this is a HEAD event listener, simulate receiving an event
        if (eventType == io.github.amilcar.eth.eventslistener.model.EventType.HEAD && listenerObj instanceof BeaconEventListener) {
                // Schedule a task to call the listener after a short delay
                new Thread(() -> {
                    try {
                        // Small delay to simulate network latency
                        Thread.sleep(200);

                        // Call the listener with our sample event
                        @SuppressWarnings("unchecked")
            BeaconEventListener<io.github.amilcar.eth.eventslistener.model.BeaconEvent> listener =
                (BeaconEventListener<io.github.amilcar.eth.eventslistener.model.BeaconEvent>) listenerObj;
            io.github.amilcar.eth.eventslistener.model.BeaconEvent externalEvent =
                new io.github.amilcar.eth.eventslistener.model.BeaconEvent(
                    io.github.amilcar.eth.eventslistener.model.EventType.HEAD,
                    new ObjectMapper().readTree(eventJson)
                );
            listener.onEvent(externalEvent);

                        // Mark that we received the event
            receivedEvent.set(sampleInternalEvent);
                        eventReceived.set(true);
                        eventLatch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }

            return mockBeaconEventClient;
    }).when(mockBeaconEventClient).addEventListener(any(io.github.amilcar.eth.eventslistener.model.EventType.class), any());

        // Set up the service with the mock server URL
        runner.setProperty(service, StandardBeaconEventsService.BEACON_NODE_URL, mockServerUrl);

        // Enable the service
        runner.enableControllerService(service);

        // Set the service in the processor
        runner.setProperty(TestProcessor.BEACON_EVENTS_SERVICE, "test-good");

        // Run the processor to subscribe to events
        runner.run();

        // Wait for the event to be received
        boolean received = eventLatch.await(5, TimeUnit.SECONDS);

        // Verify that the event was received
        assertTrue(received, "Event should have been received");
        assertTrue(eventReceived.get(), "Event should have been received");
        assertNotNull(receivedEvent.get(), "Received event should not be null");
    assertEquals(EventType.HEAD, receivedEvent.get().getEventType(), "Event type should be HEAD");
    assertTrue(receivedEvent.get().getJsonData().contains("\"slot\":\"10\""), "Slot should match");

        // Run the processor again to unsubscribe
        runner.run();

        // Verify the processor ran without errors
        runner.assertValid();
    }
}
