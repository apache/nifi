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
package org.apache.nifi.registry.event;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventHookException;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestEventService {

    private CapturingEventHook eventHook;
    private EventService eventService;

    @Before
    public void setup() {
        eventHook = new CapturingEventHook();
        eventService = new EventService(Collections.singletonList(eventHook));
        eventService.postConstruct();
    }

    @After
    public void teardown() throws Exception {
        eventService.destroy();
    }

    @Test
    public void testPublishConsume() throws InterruptedException {
        final Bucket bucket = new Bucket();
        bucket.setIdentifier(UUID.randomUUID().toString());

        final Event bucketCreatedEvent = EventFactory.bucketCreated(bucket);
        eventService.publish(bucketCreatedEvent);

        final Event bucketDeletedEvent = EventFactory.bucketDeleted(bucket);
        eventService.publish(bucketDeletedEvent);

        Thread.sleep(1000);

        final List<Event> events = eventHook.getEvents();
        Assert.assertEquals(2, events.size());

        final Event firstEvent = events.get(0);
        Assert.assertEquals(bucketCreatedEvent.getEventType(), firstEvent.getEventType());

        final Event secondEvent = events.get(1);
        Assert.assertEquals(bucketDeletedEvent.getEventType(), secondEvent.getEventType());
    }

    /**
     * Simple implementation of EventHookProvider that captures event for later verification.
     */
    private class CapturingEventHook implements EventHookProvider {

        private List<Event> events = new ArrayList<>();

        @Override
        public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {

        }

        @Override
        public void handle(Event event) throws EventHookException {
            events.add(event);
        }

        public List<Event> getEvents() {
            return events;
        }
    }

}
