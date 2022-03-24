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
package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.processors.standard.util.FlowFileAttributesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * This class provide a protocol for Wait and Notify processors to work together.
 * Once AtomicDistributedMapCacheClient is passed to this protocol, components that wish to join the notification mechanism
 * should only use methods provided by this protocol, instead of calling cache API directly.
 */
public class WaitNotifyProtocol {

    private static final Logger logger = LoggerFactory.getLogger(WaitNotifyProtocol.class);

    public static final String DEFAULT_COUNT_NAME = "default";
    public static final String CONSUMED_COUNT_NAME = "consumed";
    private static final int MAX_REPLACE_RETRY_COUNT = 5;
    private static final int REPLACE_RETRY_WAIT_MILLIS = 10;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Serializer<String> stringSerializer = (value, output) -> {
        if (value != null ) {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    };

    private final Deserializer<String> stringDeserializer = input -> input == null ? null : new String(input, StandardCharsets.UTF_8);

    public static class Signal {

        /*
         * Getter and Setter methods are needed to (de)serialize JSON even if it's not used from app code.
         */

        transient private String identifier;
        transient private AtomicCacheEntry<String, String, Object> cachedEntry;
        private Map<String, Long> counts = new HashMap<>();
        private Map<String, String> attributes = new HashMap<>();
        private int releasableCount = 0;

        public Map<String, Long> getCounts() {
            return counts;
        }

        public void setCounts(Map<String, Long> counts) {
            this.counts = counts;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public void setAttributes(Map<String, String> attributes) {
            this.attributes = attributes;
        }

        @JsonIgnore
        public long getTotalCount() {
            return counts.values().stream().mapToLong(Long::longValue).sum();
        }

        public boolean isTotalCountReached(final long targetCount) {
            return getTotalCount() >= targetCount;
        }

        public boolean isCountReached(final String counterName, final long targetCount) {
            return getCount(counterName) >= targetCount;
        }

        public long getCount(final String counterName) {
            if (counterName == null || counterName.isEmpty()) {
                return getTotalCount();
            }

            final Long count = counts.get(counterName);
            return count != null ? count : 0;
        }

        public int getReleasableCount() {
            return releasableCount;
        }

        public void setReleasableCount(int releasableCount) {
            this.releasableCount = releasableCount;
        }

        /**
         * <p>Consume accumulated notification signals to let some waiting candidates get released.</p>
         *
         * <p>This method updates state of this instance, but does not update cache storage.
         * Caller of this method is responsible for updating cache storage after processing released and waiting candidates
         * by calling {@link #replace(Signal)}. Caller should rollback what it processed with these candidates if complete call failed.</p>
         *
         * @param counterName signal counter name to consume from. If not specified, total counter is used, and 'consumed' counter is added to subtract consumed counters from total counter.
         * @param requiredCountForPass number of required signals to acquire a pass.
         * @param releasableCandidateCountPerPass number of releasable candidate per pass.
         * @param candidates candidates waiting for being allowed to pass.
         * @param released function to process allowed candidates to pass.
         * @param waiting function to process candidates those should remain in waiting queue.
         * @param <E> Type of candidate
         */
        public <E> void releaseCandidates(final String counterName, final long requiredCountForPass,
                                          final int releasableCandidateCountPerPass, final List<E> candidates,
                                          final Consumer<List<E>> released, final Consumer<List<E>> waiting) {

            final int candidateSize = candidates.size();
            if (releasableCount < candidateSize) {
                // If current passCount is not enough for the candidate size, then try to get more.
                // Convert notification signals to pass ticket.
                final long signalCount = getCount(counterName);
                releasableCount += (signalCount / requiredCountForPass) * releasableCandidateCountPerPass;
                final long reducedSignalCount = signalCount % requiredCountForPass;
                if (counterName != null && !counterName.isEmpty()) {
                    // Update target counter with reduced count.
                    counts.put(counterName, reducedSignalCount);
                } else {
                    // If target counter name is not specified, add consumed count to subtract from accumulated total count.
                    Long consumedCount = counts.getOrDefault(CONSUMED_COUNT_NAME, 0L);
                    consumedCount -= signalCount - reducedSignalCount;
                    counts.put(CONSUMED_COUNT_NAME, consumedCount);
                }
            }

            int releaseCount = Math.min(releasableCount, candidateSize);
            released.accept(candidates.subList(0, releaseCount));
            waiting.accept(candidates.subList(releaseCount, candidateSize));

            releasableCount -= releaseCount;
        }

    }

    private final AtomicDistributedMapCacheClient cache;

    public WaitNotifyProtocol(final AtomicDistributedMapCacheClient cache) {
        this.cache = cache;
    }

    /**
     * Notify a signal to increase a counter.
     * @param signalId a key in the underlying cache engine
     * @param deltas a map containing counterName and delta entries, 0 has special meaning, clears the counter back to 0
     * @param attributes attributes to save in the cache entry
     * @return A Signal instance, merged with an existing signal if any
     * @throws IOException thrown when it failed interacting with the cache engine
     * @throws ConcurrentModificationException thrown if other process is also updating the same signal and failed to update after few retry attempts
     */
    public Signal notify(final String signalId, final Map<String, Integer> deltas, final Map<String, String> attributes)
            throws IOException, ConcurrentModificationException {

        for (int i = 0; i < MAX_REPLACE_RETRY_COUNT; i++) {

            final Signal existingSignal = getSignal(signalId);
            final Signal signal = existingSignal != null ? existingSignal : new Signal();
            signal.identifier = signalId;

            if (attributes != null) {
                signal.attributes.putAll(attributes);
            }

            deltas.forEach((counterName, delta) -> {
                long count = signal.counts.containsKey(counterName) ? signal.counts.get(counterName) : 0;
                count = delta == 0 ? 0 : count + delta;
                signal.counts.put(counterName, count);
            });

            if (replace(signal)) {
                return signal;
            }

            long waitMillis = REPLACE_RETRY_WAIT_MILLIS * (i + 1);
            logger.info("Waiting for {} ms to retry... {}.{}", waitMillis, signalId, deltas);
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                final String msg = String.format("Interrupted while waiting for retrying signal [%s] counter [%s].", signalId, deltas);
                throw new ConcurrentModificationException(msg, e);
            }
        }

        final String msg = String.format("Failed to update signal [%s] counter [%s] after retrying %d times.", signalId, deltas, MAX_REPLACE_RETRY_COUNT);
        throw new ConcurrentModificationException(msg);
    }


    /**
     * Notify a signal to increase a counter.
     * @param signalId a key in the underlying cache engine
     * @param counterName specify count to update
     * @param delta delta to update a counter, 0 has special meaning, clears the counter back to 0
     * @param attributes attributes to save in the cache entry
     * @return A Signal instance, merged with an existing signal if any
     * @throws IOException thrown when it failed interacting with the cache engine
     * @throws ConcurrentModificationException thrown if other process is also updating the same signal and failed to update after few retry attempts
     */
    public Signal notify(final String signalId, final String counterName, final int delta, final Map<String, String> attributes)
            throws IOException, ConcurrentModificationException {

        final Map<String, Integer> deltas = new HashMap<>();
        deltas.put(counterName, delta);
        return notify(signalId, deltas, attributes);

    }

    /**
     * Retrieve a stored Signal in the cache engine.
     * If a caller gets satisfied with the returned Signal state and finish waiting, it should call {@link #complete(String)}
     * to complete the Wait Notify protocol.
     * @param signalId a key in the underlying cache engine
     * @return A Signal instance
     * @throws IOException thrown when it failed interacting with the cache engine
     * @throws DeserializationException thrown if the cache found is not in expected serialized format
     */
    @SuppressWarnings("unchecked")
    public Signal getSignal(final String signalId) throws IOException, DeserializationException {

        final AtomicCacheEntry<String, String, Object> entry = (AtomicCacheEntry<String, String, Object>) cache.fetch(signalId, stringSerializer, stringDeserializer);

        if (entry == null) {
            // No signal found.
            return null;
        }

        final String value = entry.getValue();

        try {
            final Signal signal = objectMapper.readValue(value, Signal.class);
            signal.identifier = signalId;
            signal.cachedEntry = entry;
            return signal;
        } catch (final JsonParseException jsonE) {
            // Try to read it as FlowFileAttributes for backward compatibility.
            try {
                final Map<String, String> attributes = new FlowFileAttributesSerializer().deserialize(value.getBytes(StandardCharsets.UTF_8));
                final Signal signal = new Signal();
                signal.identifier = signalId;
                signal.setAttributes(attributes);
                signal.getCounts().put(DEFAULT_COUNT_NAME, 1L);
                return signal;
            } catch (Exception attrE) {
                final String msg = String.format("Cached value for %s was not a serialized Signal nor FlowFileAttributes. Error messages: \"%s\", \"%s\"",
                        signalId, jsonE.getMessage(), attrE.getMessage());
                throw new DeserializationException(msg);
            }
        }
    }

    /**
     * Finish protocol and remove the cache entry.
     * @param signalId a key in the underlying cache engine
     * @throws IOException thrown when it failed interacting with the cache engine
     */
    public void complete(final String signalId) throws IOException {
        cache.remove(signalId, stringSerializer);
    }

    public boolean replace(final Signal signal) throws IOException {

        final String signalJson = objectMapper.writeValueAsString(signal);
        if (signal.cachedEntry == null) {
            signal.cachedEntry = new AtomicCacheEntry<>(signal.identifier, signalJson, null);
        } else {
            signal.cachedEntry.setValue(signalJson);
        }
        return cache.replace(signal.cachedEntry, stringSerializer, stringSerializer);

    }
}
