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

package org.apache.nifi.processors.kafka.pubsub;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

public class InFlightMessageTracker {
    private final ConcurrentMap<FlowFile, Counts> messageCountsByFlowFile = new ConcurrentHashMap<>();
    private final ConcurrentMap<FlowFile, Exception> failures = new ConcurrentHashMap<>();
    private final Object progressMutex = new Object();
    private final ComponentLog logger;

    public InFlightMessageTracker(final ComponentLog logger) {
        this.logger = logger;
    }

    public void incrementAcknowledgedCount(final FlowFile flowFile) {
        final Counts counter = messageCountsByFlowFile.computeIfAbsent(flowFile, ff -> new Counts());
        counter.incrementAcknowledgedCount();

        synchronized (progressMutex) {
            progressMutex.notify();
        }
    }

    /**
     * This method guarantees that the specified FlowFile to be transferred to
     * 'success' relationship even if it did not derive any Kafka message.
     */
    public void trackEmpty(final FlowFile flowFile) {
        messageCountsByFlowFile.putIfAbsent(flowFile, new Counts());
    }

    public int getAcknowledgedCount(final FlowFile flowFile) {
        final Counts counter = messageCountsByFlowFile.get(flowFile);
        return (counter == null) ? 0 : counter.getAcknowledgedCount();
    }

    public void incrementSentCount(final FlowFile flowFile) {
        final Counts counter = messageCountsByFlowFile.computeIfAbsent(flowFile, ff -> new Counts());
        counter.incrementSentCount();
    }

    public int getSentCount(final FlowFile flowFile) {
        final Counts counter = messageCountsByFlowFile.get(flowFile);
        return (counter == null) ? 0 : counter.getSentCount();
    }

    public void fail(final FlowFile flowFile, final Exception exception) {
        failures.putIfAbsent(flowFile, exception);
        logger.error("Failed to send " + flowFile + " to Kafka", exception);

        synchronized (progressMutex) {
            progressMutex.notify();
        }
    }

    public Exception getFailure(final FlowFile flowFile) {
        return failures.get(flowFile);
    }

    public boolean isFailed(final FlowFile flowFile) {
        return getFailure(flowFile) != null;
    }

    public void reset() {
        messageCountsByFlowFile.clear();
        failures.clear();
    }

    public PublishResult failOutstanding(final Exception exception) {
        messageCountsByFlowFile.keySet().stream()
            .filter(ff -> !isComplete(ff))
            .filter(ff -> !failures.containsKey(ff))
            .forEach(ff -> failures.put(ff, exception));

        return createPublishResult();
    }

    private boolean isComplete(final FlowFile flowFile) {
        final Counts counts = messageCountsByFlowFile.get(flowFile);
        if (counts.getAcknowledgedCount() == counts.getSentCount()) {
            // all messages received successfully.
            return true;
        }

        if (failures.containsKey(flowFile)) {
            // FlowFile failed so is complete
            return true;
        }

        return false;
    }

    private boolean isComplete() {
        return messageCountsByFlowFile.keySet().stream()
            .allMatch(flowFile -> isComplete(flowFile));
    }

    void awaitCompletion(final long millis) throws InterruptedException, TimeoutException {
        final long startTime = System.nanoTime();
        final long maxTime = startTime + TimeUnit.MILLISECONDS.toNanos(millis);

        while (System.nanoTime() < maxTime) {
            synchronized (progressMutex) {
                if (isComplete()) {
                    return;
                }

                progressMutex.wait(millis);
            }
        }

        throw new TimeoutException();
    }


    PublishResult createPublishResult() {
        return new PublishResult() {
            @Override
            public boolean isFailure() {
                return !failures.isEmpty();
            }

            @Override
            public int getSuccessfulMessageCount(final FlowFile flowFile) {
                return getAcknowledgedCount(flowFile);
            }

            @Override
            public Exception getReasonForFailure(final FlowFile flowFile) {
                return getFailure(flowFile);
            }
        };
    }

    public static class Counts {
        private final AtomicInteger sentCount = new AtomicInteger(0);
        private final AtomicInteger acknowledgedCount = new AtomicInteger(0);

        public void incrementSentCount() {
            sentCount.incrementAndGet();
        }

        public void incrementAcknowledgedCount() {
            acknowledgedCount.incrementAndGet();
        }

        public int getAcknowledgedCount() {
            return acknowledgedCount.get();
        }

        public int getSentCount() {
            return sentCount.get();
        }
    }
}
