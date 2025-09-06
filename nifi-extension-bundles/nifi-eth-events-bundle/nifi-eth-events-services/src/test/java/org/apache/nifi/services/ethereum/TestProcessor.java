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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TestProcessor.class);

    public static final PropertyDescriptor BEACON_EVENTS_SERVICE = new PropertyDescriptor.Builder()
            .name("Ethereum Beacon Chain Event Service")
            .description("Service for connecting to an Ethereum beacon chain node and listening for events")
            .identifiesControllerService(BeaconEventsService.class)
            .required(true)
            .build();

    private String subscriptionId;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        BeaconEventsService service = context.getProperty(BEACON_EVENTS_SERVICE).asControllerService(BeaconEventsService.class);

        // If we don't have a subscription yet, create one
        if (subscriptionId == null) {
            // Subscribe to all event types
            EnumSet<EventType> eventTypes = EnumSet.allOf(EventType.class);

            // Create a consumer for beacon events
            subscriptionId = service.subscribeToEvents(eventTypes, event -> {
                logger.info("Received event: {}", event);
                // Process the event here
            });

            logger.info("Subscribed to beacon events with ID: {}", subscriptionId);
        } else {
            // For testing purposes, unsubscribe after processing once
            boolean unsubscribed = service.unsubscribeFromEvents(subscriptionId);
            logger.info("Unsubscribed from beacon events with ID {}: {}", subscriptionId, unsubscribed);
            subscriptionId = null;
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(BEACON_EVENTS_SERVICE);
        return propDescs;
    }
}
