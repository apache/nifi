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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.apache.nifi.services.ethereum.EventType;
import org.apache.nifi.services.ethereum.BeaconEvent;
import org.apache.nifi.services.ethereum.BeaconEventsService;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class BeaconEventsProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(BeaconEventsProcessor.class);

        // Set up the mock service
        MockEthereumEventsService mockService = new MockEthereumEventsService();
        testRunner.addControllerService("mockEthService", mockService);
        testRunner.enableControllerService(mockService);

        // Configure the processor
        testRunner.setProperty(BeaconEventsProcessor.ETH_EVENT_SERVICE, "mockEthService");
        testRunner.setProperty(BeaconEventsProcessor.EVENT_TYPES, "HEAD,BLOCK");
    }

    @Test
    public void testJsonOutput() throws Exception {
        // Grab the mock service to send an event
        MockEthereumEventsService svc = (MockEthereumEventsService) testRunner.getControllerService("mockEthService");

        // Run once to trigger onScheduled and register subscription, keep scheduled
        testRunner.run(1, false, true);

        // Ensure we have a subscription registered
        Assertions.assertFalse(svc.subscriptions.isEmpty(), "No subscriptions registered");

        // Simulate an incoming event via the registered consumer
        String sampleJson = "{\"slot\":\"123\",\"body\":{\"foo\":\"bar\"}}";
        BeaconEvent ev = new BeaconEvent(EventType.BLOCK, sampleJson);
        svc.deliver(ev);

        // Run again (without re-initializing) to process queued event
        testRunner.run(1, false, false);

        testRunner.assertTransferCount(BeaconEventsProcessor.REL_SUCCESS, 1);
        java.util.List<org.apache.nifi.util.MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(BeaconEventsProcessor.REL_SUCCESS);
        String content = new String(flowFiles.get(0).toByteArray());

        // Expect JSON output
        Assertions.assertTrue(content.contains("\"type\":\"BLOCK\""));
        Assertions.assertTrue(content.contains("\"data\":{\"slot\":\"123\",\"body\":{\"foo\":\"bar\"}}"));
    }

    @Test
    public void testProcessorConfiguration() {
        // Verify the processor is valid as configured
        testRunner.assertValid();
    }

    @Test
    public void testProcessorWithNoInput() {
        // Run the processor once
        testRunner.run(1);

        // Verify no flowfiles were created since no events were triggered
        testRunner.assertTransferCount(BeaconEventsProcessor.REL_SUCCESS, 0);
    }

    @Test
    public void testInvalidEventType() {
        // Set an invalid event type
        testRunner.setProperty(BeaconEventsProcessor.EVENT_TYPES, "INVALID_TYPE");

    // With the stricter validator the processor should be invalid at configuration time
    testRunner.assertNotValid();
    }

    /**
     * Mock implementation of the BeaconEventsService interface for testing
     */
    private static class MockEthereumEventsService extends AbstractControllerService implements BeaconEventsService {
    final Map<String, Consumer<BeaconEvent>> subscriptions = new HashMap<>();
        private final String beaconNodeUrl = "http://localhost:5051";

        @Override
        public String subscribeToEvents(EnumSet<EventType> eventTypes, Consumer<BeaconEvent> eventConsumer) {
            String subscriptionId = UUID.randomUUID().toString();
            subscriptions.put(subscriptionId, eventConsumer);
            return subscriptionId;
        }

        @Override
        public boolean unsubscribeFromEvents(String subscriptionId) {
            return subscriptions.remove(subscriptionId) != null;
        }

        @Override
        public String getBeaconNodeUrl() {
            return beaconNodeUrl;
        }

        @Override
        public void execute() {
            // Legacy method, not used
        }

        void deliver(final BeaconEvent event) {
            subscriptions.values().forEach(c -> c.accept(event));
        }
    }
}
