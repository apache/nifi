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

package org.apache.nifi.provenance.store;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.iterator.EventIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class ArrayListEventStore implements EventStore {
    private static final Logger logger = LoggerFactory.getLogger(ArrayListEventStore.class);

    private final List<ProvenanceEventRecord> events = new ArrayList<>();
    private final AtomicLong idGenerator = new AtomicLong(0L);

    @Override
    public void close() throws IOException {
    }

    @Override
    public void initialize() throws IOException {
    }

    public StorageResult addEvent(final ProvenanceEventRecord event) {
        return addEvents(Collections.singleton(event));
    }

    @Override
    public synchronized StorageResult addEvents(Iterable<ProvenanceEventRecord> events) {
        final Map<ProvenanceEventRecord, StorageSummary> storageLocations = new HashMap<>();

        for (final ProvenanceEventRecord event : events) {
            this.events.add(event);

            final StorageSummary storageSummary = new StorageSummary(idGenerator.getAndIncrement(), "location", "1", 1, 0L, 0L);
            storageLocations.put(event, storageSummary);
        }

        return new StorageResult() {
            @Override
            public Map<ProvenanceEventRecord, StorageSummary> getStorageLocations() {
                return storageLocations;
            }

            @Override
            public boolean triggeredRollover() {
                return false;
            }

            @Override
            public Integer getEventsRolledOver() {
                return null;
            }
        };
    }

    @Override
    public long getSize() throws IOException {
        return 0;
    }

    @Override
    public long getMaxEventId() {
        return idGenerator.get() - 1;
    }

    @Override
    public synchronized Optional<ProvenanceEventRecord> getEvent(long id) throws IOException {
        if (events.size() <= id) {
            return Optional.empty();
        }

        return Optional.ofNullable(events.get((int) id));
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxResults) throws IOException {
        return getEvents(firstRecordId, maxResults, EventAuthorizer.GRANT_ALL, EventTransformer.EMPTY_TRANSFORMER);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxResults, EventAuthorizer authorizer, EventTransformer transformer) throws IOException {
        final List<ProvenanceEventRecord> events = new ArrayList<>();
        for (int i = 0; i < maxResults; i++) {
            final Optional<ProvenanceEventRecord> eventOption = getEvent(firstRecordId + i);
            if (!eventOption.isPresent()) {
                break;
            }

            events.add(eventOption.get());
        }

        return events;
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final List<Long> eventIds, final EventAuthorizer authorizer, final EventTransformer transformer) {
        final List<ProvenanceEventRecord> events = new ArrayList<>();
        for (final Long eventId : eventIds) {
            final Optional<ProvenanceEventRecord> eventOption;
            try {
                eventOption = getEvent(eventId);
            } catch (final Exception e) {
                logger.warn("Failed to retrieve event with ID " + eventId, e);
                continue;
            }

            if (!eventOption.isPresent()) {
                continue;
            }

            if (authorizer.isAuthorized(eventOption.get())) {
                events.add(eventOption.get());
            } else {
                final Optional<ProvenanceEventRecord> transformedOption = transformer.transform(eventOption.get());
                if (transformedOption.isPresent()) {
                    events.add(transformedOption.get());
                }
            }
        }

        return events;
    }

    @Override
    public void reindexLatestEvents(EventIndex eventIndex) {
    }

    @Override
    public EventIterator getEventsByTimestamp(final long minTimestamp, final long maxTimestamp) {
        return null;
    }
}
