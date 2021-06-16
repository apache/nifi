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

import org.apache.nifi.registry.hook.Event;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Service used for publishing events and passing events to the hook providers.
 */
@Service
public class EventService implements DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

    // Should only be a few events in the queue at a time, but setting a capacity just so it isn't unbounded
    static final int EVENT_QUEUE_SIZE = 10_000;

    private final BlockingQueue<Event> eventQueue;
    private final ExecutorService scheduledExecutorService;
    private final List<EventHookProvider> eventHookProviders;

    @Autowired
    public EventService(final List<EventHookProvider> eventHookProviders) {
        this.eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);
        this.scheduledExecutorService = Executors.newSingleThreadExecutor();
        this.eventHookProviders = new ArrayList<>(eventHookProviders);
    }

    @PostConstruct
    public void postConstruct() {
        LOGGER.info("Starting event consumer...");

        this.scheduledExecutorService.execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    final Event event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }

                    // event was available so notify each provider, contain errors per-provider
                    for(final EventHookProvider provider : eventHookProviders) {
                        try {
                            if (event.getEventType() == null
                                    || (event.getEventType() != null && provider.shouldHandle(event.getEventType()))) {
                                provider.handle(event);
                            }
                        } catch (Exception e) {
                            LOGGER.error("Error handling event hook", e);
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while polling event queue");
                    return;
                }
            }
        });

        LOGGER.info("Event consumer started!");
    }

    @Override
    public void destroy() throws Exception {
        LOGGER.info("Shutting down event consumer...");
        this.scheduledExecutorService.shutdownNow();
        LOGGER.info("Event consumer shutdown!");
    }

    public void publish(final Event event) {
        if (event == null) {
            return;
        }

        try {
            event.validate();

            final boolean queued = eventQueue.offer(event);
            if (!queued) {
                LOGGER.error("Unable to queue event because queue is full");
            }
        } catch (IllegalStateException e) {
            LOGGER.error("Invalid event due to: " + e.getMessage(), e);
        }
    }

}
