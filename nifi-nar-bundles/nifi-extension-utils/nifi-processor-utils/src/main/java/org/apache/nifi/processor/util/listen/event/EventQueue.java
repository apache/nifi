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
package org.apache.nifi.processor.util.listen.event;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.logging.ComponentLog;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a BlockingQueue to centralize logic for offering events across UDP, TCP, and SSL.
 *
 * @param <E> the type of event
 */
public class EventQueue<E extends Event> {

    /**
     * The default number of milliseconds to wait when offering new events to the queue.
     */
    public static final long DEFAULT_OFFER_WAIT_MS = 100;

    private final long offerWaitMs;
    private final BlockingQueue<E> events;
    private final ComponentLog logger;

    public EventQueue(final BlockingQueue<E> events, final ComponentLog logger) {
        this(events, DEFAULT_OFFER_WAIT_MS, logger);
    }

    public EventQueue(final BlockingQueue<E> events, final long offerWaitMs, final ComponentLog logger) {
        this.events = events;
        this.offerWaitMs = offerWaitMs;
        this.logger = logger;
        Validate.notNull(this.events);
        Validate.notNull(this.logger);
    }

    /**
     * Offers the given event to the events queue with a wait time, if the offer fails the event
     * is dropped an error is logged.
     *
     * @param event the event to offer
     * @throws InterruptedException if interrupted while waiting to offer
     */
    public void offer(final E event) throws InterruptedException {
        boolean queued = events.offer(event, offerWaitMs, TimeUnit.MILLISECONDS);
        if (!queued) {
            logger.error("Internal queue at maximum capacity, could not queue event");
        }
    }

}
