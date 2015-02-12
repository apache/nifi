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
package org.apache.nifi.provenance.journaling;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.SearchableFieldParser;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.StorageLocation;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.QueryUtils;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.partition.Partition;
import org.apache.nifi.provenance.journaling.partition.PartitionAction;
import org.apache.nifi.provenance.journaling.partition.PartitionManager;
import org.apache.nifi.provenance.journaling.partition.QueuingPartitionManager;
import org.apache.nifi.provenance.journaling.partition.VoidPartitionAction;
import org.apache.nifi.provenance.journaling.toc.StandardTocReader;
import org.apache.nifi.provenance.journaling.toc.TocReader;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JournalingProvenanceRepository implements ProvenanceEventRepository {
    private static final Logger logger = LoggerFactory.getLogger(JournalingProvenanceRepository.class);
    
    private final JournalingRepositoryConfig config;
    private final PartitionManager partitionManager;
    private final AtomicLong idGenerator = new AtomicLong(0L);
    
    private EventReporter eventReporter;    // effectively final
    private final ExecutorService executor;
    
    
    public JournalingProvenanceRepository() throws IOException {
        this(createConfig());
    }
    
    public JournalingProvenanceRepository(final JournalingRepositoryConfig config) throws IOException {
        this.config = config;
        this.executor = Executors.newFixedThreadPool(config.getThreadPoolSize());
        this.partitionManager = new QueuingPartitionManager(config, executor);
    }
    
    
    private static JournalingRepositoryConfig createConfig()  {
        final NiFiProperties properties = NiFiProperties.getInstance();
        final Map<String, Path> storageDirectories = properties.getProvenanceRepositoryPaths();
        if (storageDirectories.isEmpty()) {
            storageDirectories.put("provenance_repository", Paths.get("provenance_repository"));
        }
        final String storageTime = properties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_TIME, "24 hours");
        final String storageSize = properties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_SIZE, "1 GB");
        final String rolloverTime = properties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_TIME, "5 mins");
        final String rolloverSize = properties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_SIZE, "100 MB");
        final String shardSize = properties.getProperty(NiFiProperties.PROVENANCE_INDEX_SHARD_SIZE, "500 MB");
        final int queryThreads = properties.getIntegerProperty(NiFiProperties.PROVENANCE_QUERY_THREAD_POOL_SIZE, 2);
        final int journalCount = properties.getIntegerProperty(NiFiProperties.PROVENANCE_JOURNAL_COUNT, 16);

        final long storageMillis = FormatUtils.getTimeDuration(storageTime, TimeUnit.MILLISECONDS);
        final long maxStorageBytes = DataUnit.parseDataSize(storageSize, DataUnit.B).longValue();
        final long rolloverMillis = FormatUtils.getTimeDuration(rolloverTime, TimeUnit.MILLISECONDS);
        final long rolloverBytes = DataUnit.parseDataSize(rolloverSize, DataUnit.B).longValue();

        final boolean compressOnRollover = Boolean.parseBoolean(properties.getProperty(NiFiProperties.PROVENANCE_COMPRESS_ON_ROLLOVER));
        final String indexedFieldString = properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS);
        final String indexedAttrString = properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES);

        final Boolean alwaysSync = Boolean.parseBoolean(properties.getProperty("nifi.provenance.repository.always.sync", "false"));

        final List<SearchableField> searchableFields = SearchableFieldParser.extractSearchableFields(indexedFieldString, true);
        final List<SearchableField> searchableAttributes = SearchableFieldParser.extractSearchableFields(indexedAttrString, false);

        // We always want to index the Event Time.
        if (!searchableFields.contains(SearchableFields.EventTime)) {
            searchableFields.add(SearchableFields.EventTime);
        }

        final JournalingRepositoryConfig config = new JournalingRepositoryConfig();
        
        final Map<String, File> containers = new HashMap<>(storageDirectories.size());
        for ( final Map.Entry<String, Path> entry : storageDirectories.entrySet() ) {
            containers.put(entry.getKey(), entry.getValue().toFile());
        }
        config.setContainers(containers);
        config.setCompressOnRollover(compressOnRollover);
        config.setSearchableFields(searchableFields);
        config.setSearchableAttributes(searchableAttributes);
        config.setJournalCapacity(rolloverBytes);
        config.setJournalRolloverPeriod(rolloverMillis, TimeUnit.MILLISECONDS);
        config.setEventExpiration(storageMillis, TimeUnit.MILLISECONDS);
        config.setMaxStorageCapacity(maxStorageBytes);
        config.setThreadPoolSize(queryThreads);
        config.setPartitionCount(journalCount);

        if (shardSize != null) {
            config.setDesiredIndexSize(DataUnit.parseDataSize(shardSize, DataUnit.B).longValue());
        }

        config.setAlwaysSync(alwaysSync);

        return config;
    }
    
    @Override
    public synchronized void initialize(final EventReporter eventReporter) throws IOException {
        this.eventReporter = eventReporter;
    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) throws IOException {
        registerEvents(Collections.singleton(event));
    }

    @Override
    public void registerEvents(final Collection<ProvenanceEventRecord> events) throws IOException {
        partitionManager.withPartition(new VoidPartitionAction() {
            @Override
            public void perform(final Partition partition) throws IOException {
                partition.registerEvents(events, idGenerator.getAndAdd(events.size()));
            }
        }, true);
    }

    @Override
    public StoredProvenanceEvent getEvent(final long id) throws IOException {
        final List<StoredProvenanceEvent> events = getEvents(id, 1);
        if ( events.isEmpty() ) {
            return null;
        }

        // We have to check the id of the event returned, because we are requesting up to 1 record
        // starting with the given id. However, if that ID doesn't exist, we could get a record
        // with a larger id.
        final StoredProvenanceEvent event = events.get(0);
        if ( event.getEventId() == id ) {
            return event;
        }
        
        return null;
    }
    
    @Override
    public List<StoredProvenanceEvent> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        // Must generate query to determine the appropriate StorageLocation objects and then call
        // getEvent(List<StorageLocation>)
        final Set<List<JournaledStorageLocation>> resultSet = partitionManager.withEachPartition(
            new PartitionAction<List<JournaledStorageLocation>>() {
                @Override
                public List<JournaledStorageLocation> perform(final Partition partition) throws IOException {
                    return partition.getEvents(firstRecordId, maxRecords);
                }
        });
        
        final ArrayList<JournaledStorageLocation> locations = new ArrayList<>(maxRecords);
        for ( final List<JournaledStorageLocation> list : resultSet ) {
            for ( final JournaledStorageLocation location : list ) {
                locations.add(location);
            }
        }
        
        Collections.sort(locations, new Comparator<JournaledStorageLocation>() {
            @Override
            public int compare(final JournaledStorageLocation o1, final JournaledStorageLocation o2) {
                return Long.compare(o1.getEventId(), o2.getEventId());
            }
        });
        
        locations.trimToSize();
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        final List<StorageLocation> storageLocations = (List<StorageLocation>) ((List) locations);
        return getEvents(storageLocations);
    }
    
    @Override
    public StoredProvenanceEvent getEvent(final StorageLocation location) throws IOException {
        final List<StoredProvenanceEvent> storedEvents = getEvents(Collections.singletonList(location));
        return (storedEvents == null || storedEvents.isEmpty()) ? null : storedEvents.get(0);
    }
    
    
    
    @Override
    public List<StoredProvenanceEvent> getEvents(final List<StorageLocation> locations) throws IOException {
        // Group the locations by journal files because we want a single thread, at most, per journal file.
        final Map<File, List<JournaledStorageLocation>> orderedLocations = QueryUtils.orderLocations(locations, config);
        
        // Go through each journal file and create a callable that can lookup the records for that journal file.
        final List<Future<List<StoredProvenanceEvent>>> futures = new ArrayList<>();
        for ( final Map.Entry<File, List<JournaledStorageLocation>> entry : orderedLocations.entrySet() ) {
            final File journalFile = entry.getKey();
            final List<JournaledStorageLocation> locationsForFile = entry.getValue();
            
            final Callable<List<StoredProvenanceEvent>> callable = new Callable<List<StoredProvenanceEvent>>() {
                @Override
                public List<StoredProvenanceEvent> call() throws Exception {
                    try(final TocReader tocReader = new StandardTocReader(new File(journalFile.getParentFile(), journalFile.getName() + ".toc"));
                        final JournalReader reader = new StandardJournalReader(journalFile)) 
                    {
                        final List<StoredProvenanceEvent> storedEvents = new ArrayList<>(locationsForFile.size());
                        
                        for ( final JournaledStorageLocation location : locationsForFile ) {
                            final long blockOffset = tocReader.getBlockOffset(location.getBlockIndex());
                            final ProvenanceEventRecord event = reader.getEvent(blockOffset, location.getEventId());
                            
                            storedEvents.add(new JournaledProvenanceEvent(event, location));
                        }
                        
                        return storedEvents;
                    }
                }
            };
            
            final Future<List<StoredProvenanceEvent>> future = executor.submit(callable);
            futures.add(future);
        }
        
        // Get all of the events from the futures, waiting for them to finish.
        final Map<StorageLocation, StoredProvenanceEvent> locationToEventMap = new HashMap<>(locations.size());
        for ( final Future<List<StoredProvenanceEvent>> future : futures ) {
            try {
                final List<StoredProvenanceEvent> events = future.get();
                
                // Map the location to the event, so that we can then re-order the events in the same order
                // that the locations were passed to us.
                for ( final StoredProvenanceEvent event : events ) {
                    locationToEventMap.put(event.getStorageLocation(), event);
                }
            } catch (final ExecutionException ee) {
                final Throwable cause = ee.getCause();
                if ( cause instanceof IOException ) {
                    throw (IOException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            } catch (final InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        
        // Sort Events by the order of the provided locations.
        final List<StoredProvenanceEvent> sortedEvents = new ArrayList<>(locations.size());
        for ( final StorageLocation location : locations ) {
            final StoredProvenanceEvent event = locationToEventMap.get(location);
            if ( event != null ) {
                sortedEvents.add(event);
            }
        }
        
        return sortedEvents;
    }
    

    @Override
    public Long getMaxEventId() throws IOException {
        final Set<Long> maxIds = partitionManager.withEachPartition(new PartitionAction<Long>() {
            @Override
            public Long perform(final Partition partition) throws IOException {
                return partition.getMaxEventId();
            }
        });
        
        Long maxId = null;
        for ( final Long id : maxIds ) {
            if ( id == null ) {
                continue;
            }
            
            if ( maxId == null || id > maxId ) {
                maxId = id;
            }
        }
        
        return maxId;
    }

    
    @Override
    public QuerySubmission submitQuery(final Query query) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(final String lineageIdentifier) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws IOException {
        partitionManager.shutdown();
        executor.shutdown();
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return config.getSearchableFields();
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return config.getSearchableAttributes();
    }

    @Override
    public Long getEarliestEventTime() throws IOException {
        // Get the earliest event timestamp for each partition
        final Set<Long> earliestTimes = partitionManager.withEachPartition(new PartitionAction<Long>() {
            @Override
            public Long perform(final Partition partition) throws IOException {
                return partition.getEarliestEventTime();
            }
        });
        
        // Find the latest timestamp for each of the "earliest" timestamps.
        // This is a bit odd, but we're doing it for a good reason:
        //      The UI is going to show the earliest time available. Because we have a partitioned write-ahead
        //      log, if we just return the timestamp of the earliest event available, we could end up returning
        //      a time for an event that exists but the next event in its lineage does not exist because it was
        //      already aged off of a different journal. To avoid this, we return the "latest of the earliest"
        //      timestamps. This way, we know that no event with a larger ID has been aged off from any of the
        //      partitions.
        Long latest = null;
        for ( final Long earliestTime : earliestTimes ) {
            if ( earliestTime == null ) {
                continue;
            }
            
            if ( latest == null || earliestTime > latest ) {
                latest = earliestTime;
            }
        }
        
        return latest;
    }

}
