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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.lucene.util.NamedThreadFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.store.iterator.AuthorizingEventIterator;
import org.apache.nifi.provenance.store.iterator.EventIterator;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PartitionedEventStore implements EventStore {
    private static final Logger logger = LoggerFactory.getLogger(PartitionedEventStore.class);
    private static final String EVENT_CATEGORY = "Provenance Repository";

    private final AtomicLong partitionIndex = new AtomicLong(0L);
    private final RepositoryConfiguration repoConfig;
    private final EventReporter eventReporter;
    private ScheduledExecutorService maintenanceExecutor;

    public PartitionedEventStore(final RepositoryConfiguration config, final EventReporter eventReporter) {
        this.repoConfig = config;
        this.eventReporter = eventReporter;
    }


    @Override
    public void initialize() throws IOException {
        maintenanceExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Provenance Repository Maintenance"));
        maintenanceExecutor.scheduleWithFixedDelay(() -> performMaintenance(), 1, 1, TimeUnit.MINUTES);

        for (final EventStorePartition partition : getPartitions()) {
            partition.initialize();
        }
    }

    @Override
    public void close() throws IOException {
        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdownNow();
        }

        IOException thrown = null;

        for (final EventStorePartition partition : getPartitions()) {
            try {
               partition.close();
            } catch (final IOException ioe) {
                if (thrown == null) {
                    thrown = ioe;
                } else {
                    thrown.addSuppressed(ioe);
                }
            }
        }

        if (thrown != null) {
            throw thrown;
        }
    }


    @Override
    public StorageResult addEvents(final Iterable<ProvenanceEventRecord> events) throws IOException {
        final List<? extends EventStorePartition> partitions = getPartitions();
        final int index = (int) (partitionIndex.getAndIncrement() % partitions.size());
        final EventStorePartition partition = partitions.get(index);
        return partition.addEvents(events);
    }

    @Override
    public long getSize() {
        long size = 0;
        for (final EventStorePartition partition : getPartitions()) {
            size += partition.getSize();
        }

        return size;
    }

    private long getRepoSize() {
        long total = 0L;

        for (final File storageDir : repoConfig.getStorageDirectories().values()) {
            total += DirectoryUtils.getSize(storageDir);
        }

        return total;
    }

    @Override
    public long getMaxEventId() {
        return getPartitions().stream()
            .mapToLong(part -> part.getMaxEventId())
            .max()
            .orElse(-1L);
    }

    @Override
    public Optional<ProvenanceEventRecord> getEvent(final long id) throws IOException {
        for (final EventStorePartition partition : getPartitions()) {
            final Optional<ProvenanceEventRecord> option = partition.getEvent(id);
            if (option.isPresent()) {
                return option;
            }
        }

        return Optional.empty();
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        return getEvents(firstRecordId, maxRecords, EventAuthorizer.GRANT_ALL, EventTransformer.EMPTY_TRANSFORMER);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final EventAuthorizer authorizer,
        final EventTransformer transformer) throws IOException {
        if (firstRecordId + maxRecords < 1 || maxRecords < 1 || firstRecordId > getMaxEventId()) {
            return Collections.emptyList();
        }

        return getEvents(maxRecords, authorizer, part -> part.createEventIterator(firstRecordId), transformer);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final List<Long> eventIds, final EventAuthorizer authorizer, final EventTransformer transformer) throws IOException {
        if (eventIds == null || eventIds.isEmpty()) {
            return Collections.emptyList();
        }

        return getEvents(eventIds.size(), authorizer, part -> part.createEventIterator(eventIds), transformer);
    }

    private List<ProvenanceEventRecord> getEvents(final int maxRecords, final EventAuthorizer authorizer,
        final Function<EventStorePartition, EventIterator> eventIteratorFactory, final EventTransformer transformer) throws IOException {

        if (maxRecords < 1) {
            return Collections.emptyList();
        }

        final List<ProvenanceEventRecord> selectedEvents = new ArrayList<>();

        // Create a Map so that the key is the next record available from a partition and the value is the EventIterator from which
        // the record came. This sorted map is then used so that we are able to always get the first entry, which is the next
        // lowest record id among all partitions.
        final SortedMap<ProvenanceEventRecord, EventIterator> recordToIteratorMap = new TreeMap<>(
            (o1, o2) -> Long.compare(o1.getEventId(), o2.getEventId()));

        try {
            // Seed our map with the first event in each Partition.
            for (final EventStorePartition partition : getPartitions()) {
                final EventAuthorizer nonNullAuthorizer = authorizer == null ? EventAuthorizer.GRANT_ALL : authorizer;
                final EventIterator partitionIterator = eventIteratorFactory.apply(partition);
                final EventIterator iterator = new AuthorizingEventIterator(partitionIterator, nonNullAuthorizer, transformer);

                final Optional<ProvenanceEventRecord> option = iterator.nextEvent();
                if (option.isPresent()) {
                    recordToIteratorMap.put(option.get(), iterator);
                }
            }

            // If no records found, just return the empty list.
            if (recordToIteratorMap.isEmpty()) {
                return selectedEvents;
            }

            // Get the event with the next-lowest ID. Add it to the list of selected events,
            // then read the next event from the same EventIterator that this event came from.
            // This ensures that our map is always populated with the next event for each
            // EventIterator, which also ensures that the first key in our map is the event
            // with the lowest ID (since all events from a given EventIterator have monotonically
            // increasing Event ID's).
            ProvenanceEventRecord nextEvent = recordToIteratorMap.firstKey();
            while (nextEvent != null && selectedEvents.size() < maxRecords) {
                selectedEvents.add(nextEvent);

                final EventIterator iterator = recordToIteratorMap.remove(nextEvent);
                final Optional<ProvenanceEventRecord> nextRecordFromIterator = iterator.nextEvent();
                if (nextRecordFromIterator.isPresent()) {
                    recordToIteratorMap.put(nextRecordFromIterator.get(), iterator);
                }

                nextEvent = recordToIteratorMap.isEmpty() ? null : recordToIteratorMap.firstKey();
            }

            return selectedEvents;
        } finally {
            // Ensure that we close all record readers that have been created
            for (final EventIterator iterator : recordToIteratorMap.values()) {
                try {
                    iterator.close();
                } catch (final Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.warn("Failed to close Record Reader {}", iterator, e);
                    } else {
                        logger.warn("Failed to close Record Reader {}", iterator);
                    }
                }
            }
        }
    }


    void performMaintenance() {
        try {
            final long maxFileLife = repoConfig.getMaxRecordLife(TimeUnit.MILLISECONDS);
            for (final EventStorePartition partition : getPartitions()) {
                try {
                    partition.purgeOldEvents(maxFileLife, TimeUnit.MILLISECONDS);
                } catch (final Exception e) {
                    logger.error("Failed to purge expired events from " + partition, e);
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY,
                        "Failed to purge expired events from Provenance Repository. See logs for more information.");
                }
            }

            final long maxStorageCapacity = repoConfig.getMaxStorageCapacity();
            long currentSize;
            try {
                currentSize = getRepoSize();
            } catch (final Exception e) {
                logger.error("Could not determine size of Provenance Repository. Will not expire any data due to storage limits", e);
                eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to determine size of Provenance Repository. "
                    + "No data will be expired due to storage limits at this time. See logs for more information.");
                return;
            }

            while (currentSize > maxStorageCapacity) {
                for (final EventStorePartition partition : getPartitions()) {
                    try {
                        final long removed = partition.purgeOldestEvents();
                        currentSize -= removed;
                    } catch (final Exception e) {
                        logger.error("Failed to purge oldest events from " + partition, e);
                        eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY,
                            "Failed to purge oldest events from Provenance Repository. See logs for more information.");
                    }
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to perform periodic maintenance", e);
            eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY,
                "Failed to perform periodic maintenace for Provenance Repository. See logs for more information.");
        }
    }

    protected abstract List<? extends EventStorePartition> getPartitions();
}
