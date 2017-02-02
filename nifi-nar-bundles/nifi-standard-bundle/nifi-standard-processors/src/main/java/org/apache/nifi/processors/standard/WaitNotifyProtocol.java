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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient.CacheEntry;
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
import java.util.Map;

/**
 * This class provide a protocol for Wait and Notify processors to work together.
 * Once AtomicDistributedMapCacheClient is passed to this protocol, components that wish to join the notification mechanism
 * should only use methods provided by this protocol, instead of calling cache API directly.
 */
public class WaitNotifyProtocol {

    private static final Logger logger = LoggerFactory.getLogger(WaitNotifyProtocol.class);

    public static final String DEFAULT_COUNT_NAME = "default";
    private static final int MAX_REPLACE_RETRY_COUNT = 5;
    private static final int REPLACE_RETRY_WAIT_MILLIS = 10;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Serializer<String> stringSerializer = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
    private final Deserializer<String> stringDeserializer = input -> new String(input, StandardCharsets.UTF_8);

    public static class Signal {
        private Map<String, Long> counts = new HashMap<>();
        private Map<String, String> attributes = new HashMap<>();

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

        public boolean isTotalCountReached(final long targetCount) {
            final long totalCount = counts.values().stream().mapToLong(Long::longValue).sum();
            return totalCount >= targetCount;
        }

        public boolean isCountReached(final String counterName, final long targetCount) {
            return getCount(counterName) >= targetCount;
        }

        public long getCount(final String counterName) {
            final Long count = counts.get(counterName);
            return count != null ? count : 0;
        }

    }

    private final AtomicDistributedMapCacheClient cache;

    public WaitNotifyProtocol(final AtomicDistributedMapCacheClient cache) {
        this.cache = cache;
    }

    /**
     * Notify a signal to increase a counter.
     * @param signalId a key in the underlying cache engine
     * @param deltas a map containing counterName and delta entries
     * @param attributes attributes to save in the cache entry
     * @return A Signal instance, merged with an existing signal if any
     * @throws IOException thrown when it failed interacting with the cache engine
     * @throws ConcurrentModificationException thrown if other process is also updating the same signal and failed to update after few retry attempts
     */
    public Signal notify(final String signalId, final Map<String, Integer> deltas, final Map<String, String> attributes)
            throws IOException, ConcurrentModificationException {

        for (int i = 0; i < MAX_REPLACE_RETRY_COUNT; i++) {

            final CacheEntry<String, String> existingEntry = cache.fetch(signalId, stringSerializer, stringDeserializer);

            final Signal existingSignal = getSignal(signalId);
            final Signal signal = existingSignal != null ? existingSignal : new Signal();

            if (attributes != null) {
                signal.attributes.putAll(attributes);
            }

            deltas.forEach((counterName, delta) -> {
                long count = signal.counts.containsKey(counterName) ? signal.counts.get(counterName) : 0;
                count += delta;
                signal.counts.put(counterName, count);
            });

            final String signalJson = objectMapper.writeValueAsString(signal);
            final long revision = existingEntry != null ? existingEntry.getRevision() : -1;


            if (cache.replace(signalId, signalJson, stringSerializer, stringSerializer, revision)) {
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
     * @param delta delta to update a counter
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
    public Signal getSignal(final String signalId) throws IOException, DeserializationException {

        final CacheEntry<String, String> entry = cache.fetch(signalId, stringSerializer, stringDeserializer);

        if (entry == null) {
            // No signal found.
            return null;
        }

        final String value = entry.getValue();

        try {
            return objectMapper.readValue(value, Signal.class);
        } catch (final JsonParseException jsonE) {
            // Try to read it as FlowFileAttributes for backward compatibility.
            try {
                final Map<String, String> attributes = new FlowFileAttributesSerializer().deserialize(value.getBytes(StandardCharsets.UTF_8));
                final Signal signal = new Signal();
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
}
