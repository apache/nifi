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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class EventBatcher<E extends ByteArrayMessage> {

    public static final int POLL_TIMEOUT_MS = 20;

    private final BlockingQueue<E> events;
    private final BlockingQueue<E> errorEvents;
    private final ComponentLog logger;

    public EventBatcher(final ComponentLog logger, final BlockingQueue<E> events, final BlockingQueue<E> errorEvents) {
        this.logger = logger;
        this.events = events;
        this.errorEvents = errorEvents;
    }

    /**
     * Batches together up to the batchSize events. Events are grouped together based on a batch key which
     * by default is the sender of the event, but can be overriden by sub-classes.
     * <p>
     * This method will return when batchSize has been reached, or when no more events are available on the queue.
     *
     * @param session                the current session
     * @param totalBatchSize         the total number of events to process
     * @param messageDemarcatorBytes the demarcator to put between messages when writing to a FlowFile
     * @return a Map from the batch key to the FlowFile and events for that batch, the size of events in all
     * the batches will be <= batchSize
     */
    public Map<String, FlowFileEventBatch<E>> getBatches(final ProcessSession session, final int totalBatchSize,
                                                      final byte[] messageDemarcatorBytes) {

        final Map<String, FlowFileEventBatch<E>> batches = new HashMap<>();
        for (int i = 0; i < totalBatchSize; i++) {
            final E event = getMessage(true, true, session);
            if (event == null) {
                break;
            }

            final String batchKey = getBatchKey(event);
            FlowFileEventBatch<E> batch = batches.get(batchKey);

            // if we don't have a batch for this key then create a new one
            if (batch == null) {
                batch = new FlowFileEventBatch<>(session.create(), new ArrayList<>());
                batches.put(batchKey, batch);
            }

            // add the current event to the batch
            batch.getEvents().add(event);

            // append the event's data to the FlowFile, write the demarcator first if not on the first event
            final boolean writeDemarcator = (i > 0);
            try {
                final byte[] rawMessage = event.getMessage();
                FlowFile appendedFlowFile = session.append(batch.getFlowFile(), out -> {
                    if (writeDemarcator) {
                        out.write(messageDemarcatorBytes);
                    }

                    out.write(rawMessage);
                });

                // update the FlowFile reference in the batch object
                batch.setFlowFile(appendedFlowFile);

            } catch (final Exception e) {
                logger.error("Failed to write contents of the message to FlowFile due to {}; will re-queue message and try again",
                        e.getMessage(), e);
                errorEvents.add(event);
                break;
            }
        }

        return batches;
    }

    /**
     * The implementation should generate the indexing key for the event, to allow batching together related events.
     * Typically the batch key will be the sender IP + port to allow batching events from the same sender into a single
     * flow file.
     * @param event Use information from the event to generate a batching key
     * @return The key to batch like-kind events together eg. sender ID/socket
     */
    protected abstract String getBatchKey(E event);

    /**
     * If pollErrorQueue is true, the error queue will be checked first and event will be
     * returned from the error queue if available.
     *
     * If pollErrorQueue is false, or no data is in the error queue, the regular queue is polled.
     *
     * If longPoll is true, the regular queue will be polled with a short timeout, otherwise it will
     * poll with no timeout which will return immediately.
     *
     * @param longPoll whether or not to poll the main queue with a small timeout
     * @param pollErrorQueue whether or not to poll the error queue first
     *
     * @return an event from one of the queues, or null if none are available
     */
    protected E getMessage(final boolean longPoll, final boolean pollErrorQueue, final ProcessSession session) {
        E event = null;
        if (pollErrorQueue) {
            event = errorEvents.poll();
        }

        if (event != null) {
            return event;
        }

        try {
            if (longPoll) {
                event = events.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } else {
                event = events.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        if (event != null) {
            session.adjustCounter("Messages Received", 1L, false);
        }

        return event;
    }

}