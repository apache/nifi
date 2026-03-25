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

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Standalone (single-node) implementation of {@link EventService}.
 *
 * <p>Events are placed on an in-memory {@link BlockingQueue} and consumed by a
 * single background thread that delivers them to every configured
 * {@link EventHookProvider}.
 *
 * <p>This implementation is selected when {@code nifi.registry.cluster.enabled}
 * is {@code false} (the default). It is instantiated by
 * {@link EventServiceConfiguration}.
 */
public class StandardEventService implements EventService, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardEventService.class);

    // Generous capacity so the in-memory queue never blocks normal operations.
    static final int EVENT_QUEUE_SIZE = 10_000;

    private final BlockingQueue<Event> eventQueue;
    private final ExecutorService consumerExecutor;
    private final List<EventHookProvider> eventHookProviders;

    public StandardEventService(final List<EventHookProvider> eventHookProviders) {
        this.eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);
        this.consumerExecutor = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(r, "EventConsumer");
            t.setDaemon(true);
            return t;
        });
        this.eventHookProviders = new ArrayList<>(eventHookProviders);
    }

    @PostConstruct
    public void postConstruct() {
        LOGGER.info("Starting StandardEventService event consumer.");

        consumerExecutor.execute(() -> {
            while (!Thread.interrupted()) {
                try {
                    final Event event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                    deliverToProviders(event);
                } catch (final InterruptedException e) {
                    LOGGER.warn("StandardEventService consumer interrupted.");
                    return;
                }
            }
        });

        LOGGER.info("StandardEventService event consumer started.");
    }

    @Override
    public void destroy() {
        LOGGER.info("Shutting down StandardEventService event consumer.");
        consumerExecutor.shutdownNow();
    }

    @Override
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
        } catch (final IllegalStateException e) {
            LOGGER.error("Invalid event: {}", e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void deliverToProviders(final Event event) {
        for (final EventHookProvider provider : eventHookProviders) {
            try {
                if (event.getEventType() == null || provider.shouldHandle(event.getEventType())) {
                    provider.handle(event);
                }
            } catch (final Exception e) {
                LOGGER.error("Error handling event hook", e);
            }
        }
    }
}
