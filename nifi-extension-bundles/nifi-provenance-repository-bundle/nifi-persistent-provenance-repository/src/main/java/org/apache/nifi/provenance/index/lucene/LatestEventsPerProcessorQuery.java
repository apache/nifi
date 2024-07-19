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

package org.apache.nifi.provenance.index.lucene;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.util.RingBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LatestEventsPerProcessorQuery implements CachedQuery {
    private static final String COMPONENT_ID_FIELD_NAME = SearchableFields.ComponentID.getSearchableFieldName();
    // Map of component ID to a RingBuffer holding up to the last 1000 events
    private final ConcurrentMap<String, RingBuffer<Long>> latestRecords = new ConcurrentHashMap<>();

    // Map of component ID to a List of the Event IDs for all events in the latest batch of events that have been indexed for the given component ID
    private final ConcurrentMap<String, List<Long>> latestEventSet = new ConcurrentHashMap<>();

    @Override
    public void update(final Map<ProvenanceEventRecord, StorageSummary> events) {
        final Map<String, List<Long>> eventsByComponent = new HashMap<>();

        for (final Map.Entry<ProvenanceEventRecord, StorageSummary> entry : events.entrySet()) {
            update(entry.getKey(), entry.getValue());

            final String componentId = entry.getKey().getComponentId();
            final List<Long> eventSet = eventsByComponent.computeIfAbsent(componentId, id -> new ArrayList<>());
            eventSet.add(entry.getValue().getEventId());
        }

        latestEventSet.putAll(eventsByComponent);
    }

    private void update(final ProvenanceEventRecord event, final StorageSummary storageSummary) {
        final String componentId = event.getComponentId();
        final RingBuffer<Long> ringBuffer = latestRecords.computeIfAbsent(componentId, id -> new RingBuffer<>(1000));
        ringBuffer.add(storageSummary.getEventId());
    }

    public List<Long> getLatestEventIds(final String componentId) {
        final List<Long> eventIds = latestEventSet.get(componentId);
        return eventIds == null ? List.of() : eventIds;
    }

    @Override
    public Optional<List<Long>> evaluate(final Query query) {
        if (query.getMaxResults() > 1000) {
            // If query max results > 1000 then we know we don't have enough results. So just return empty.
            return Optional.empty();
        }

        final List<SearchTerm> terms = query.getSearchTerms();
        if (terms.size() != 1) {
            return Optional.empty();
        }

        final SearchTerm term = terms.get(0);
        if (!COMPONENT_ID_FIELD_NAME.equals(term.getSearchableField().getSearchableFieldName())) {
            return Optional.empty();
        }

        if (query.getEndDate() != null || query.getStartDate() != null) {
            return Optional.empty();
        }

        final RingBuffer<Long> ringBuffer = latestRecords.get(term.getValue());
        if (ringBuffer == null || ringBuffer.getSize() < query.getMaxResults()) {
            return Optional.empty();
        }

        List<Long> eventIds = ringBuffer.asList();
        if (eventIds.size() > query.getMaxResults()) {
            eventIds = eventIds.subList(0, query.getMaxResults());
        }

        return Optional.of(eventIds);
    }

    @Override
    public String toString() {
        return "Latest Events Per Processor";
    }

}
