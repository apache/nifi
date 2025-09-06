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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

import java.util.EnumSet;
import java.util.function.Consumer;

@Tags({"ethereum", "eth2", "beacon", "blockchain", "events", "controller", "service"})
@CapabilityDescription("Service for connecting to an Ethereum beacon chain node and listening for events. " +
        "This service allows processors to subscribe to specific event types and receive notifications when those events occur. " +
        "It uses the java-eth-events-listener library to connect to the beacon chain node and listen for events.")
public interface BeaconEventsService extends ControllerService {

    /**
     * Subscribes to beacon chain events of the specified types.
     * @param eventTypes The types of events to subscribe to
     * @param eventConsumer The consumer that will process the events
     * @return A subscription ID that can be used to unsubscribe
     */
    String subscribeToEvents(EnumSet<EventType> eventTypes, Consumer<BeaconEvent> eventConsumer);

    /**
     * Unsubscribes from events using the subscription ID.
     * @param subscriptionId The subscription ID returned from subscribeToEvents
     * @return true if the subscription was found and removed, false otherwise
     */
    boolean unsubscribeFromEvents(String subscriptionId);

    /**
     * Gets the URL of the beacon chain node.
     * @return The URL of the beacon chain node
     */
    String getBeaconNodeUrl();

    /**
     * Legacy method for backward compatibility.
     * @deprecated Use {@link #subscribeToEvents(EnumSet, Consumer)} instead.
     */
    @Deprecated
    void execute();
}
