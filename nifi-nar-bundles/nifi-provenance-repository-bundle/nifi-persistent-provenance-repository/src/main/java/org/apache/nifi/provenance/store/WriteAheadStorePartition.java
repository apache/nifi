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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.iterator.EventIterator;
import org.apache.nifi.provenance.store.iterator.SelectiveRecordReaderEventIterator;
import org.apache.nifi.provenance.store.iterator.SequentialRecordReaderEventIterator;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.apache.nifi.provenance.util.NamedThreadFactory;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadStorePartition implements EventStorePartition {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadStorePartition.class);


    private final RepositoryConfiguration config;
    private final File partitionDirectory;
    private final String partitionName;
    private final RecordWriterFactory recordWriterFactory;
    private final RecordReaderFactory recordReaderFactory;
    private final BlockingQueue<File> filesToCompress;
    private final AtomicLong idGenerator;
    private final AtomicLong maxEventId = new AtomicLong(-1L);
    private volatile boolean closed = false;

    private AtomicReference<RecordWriterLease> eventWriterLeaseRef = new AtomicReference<>();

    private final SortedMap<Long, File> minEventIdToPathMap = new TreeMap<>();  // guarded by synchronizing on object

    public WriteAheadStorePartition(final File storageDirectory, final String partitionName, final RepositoryConfiguration repoConfig, final RecordWriterFactory recordWriterFactory,
        final RecordReaderFactory recordReaderFactory, final BlockingQueue<File> filesToCompress, final AtomicLong idGenerator, final EventReporter eventReporter) {

        this.partitionName = partitionName;
        this.config = repoConfig;
        this.idGenerator = idGenerator;
        this.partitionDirectory = storageDirectory;
        this.recordWriterFactory = recordWriterFactory;
        this.recordReaderFactory = recordReaderFactory;
        this.filesToCompress = filesToCompress;
    }

    @Override
    public void close() throws IOException {
        closed = true;

        final RecordWriterLease lease = eventWriterLeaseRef.get();
        if (lease != null) {
            lease.close();
        }
    }

    @Override
    public synchronized void initialize() throws IOException {
        if (!partitionDirectory.exists()) {
            Files.createDirectories(partitionDirectory.toPath());
        }

        final File[] files = partitionDirectory.listFiles(DirectoryUtils.EVENT_FILE_FILTER);
        if (files == null) {
            throw new IOException("Could not access files in the " + partitionDirectory + " directory");
        }

        // We need to determine what the largest Event ID is in this partition. To do this, we
        // iterate over all files starting with the file that has the greatest ID, and try to find
        // the largest Event ID in that file. Once we successfully determine the greatest Event ID
        // in any one of the files, we are done, since we are iterating over the files in order of
        // the Largest Event ID to the smallest.
        long maxEventId = -1L;
        final List<File> fileList = Arrays.asList(files);
        Collections.sort(fileList, DirectoryUtils.LARGEST_ID_FIRST);
        for (final File file : fileList) {
            try {
                final RecordReader reader = recordReaderFactory.newRecordReader(file, Collections.emptyList(), Integer.MAX_VALUE);
                final long eventId = reader.getMaxEventId();
                if (eventId > maxEventId) {
                    maxEventId = eventId;
                    break;
                }
            } catch (final Exception e) {
                logger.warn("Could not read file {}; if this file contains Provenance Events, new events may be created with the same event identifiers", file, e);
            }
        }

        synchronized (minEventIdToPathMap) {
            for (final File file : fileList) {
                final long minEventId = DirectoryUtils.getMinId(file);
                minEventIdToPathMap.put(minEventId, file);
            }
        }

        this.maxEventId.set(maxEventId);

        // If configured to compress, compress any files that are not yet compressed.
        if (config.isCompressOnRollover()) {
            final File[] uncompressedFiles = partitionDirectory.listFiles(f -> f.getName().endsWith(".prov"));
            if (uncompressedFiles != null) {
                for (final File file : uncompressedFiles) {
                    // If we have both a compressed file and an uncompressed file for the same .prov file, then
                    // we must have been in the process of compressing it when NiFi was restarted. Delete the partial
                    // .gz file and we will start compressing it again.
                    final File compressed = new File(file.getParentFile(), file.getName() + ".gz");
                    if (compressed.exists()) {
                        compressed.delete();
                    }
                }
            }
        }

        // Update the ID Generator to the max of the ID Generator or maxEventId
        final long nextPartitionId = maxEventId + 1;
        final long updatedId = idGenerator.updateAndGet(curVal -> Math.max(curVal, nextPartitionId));
        logger.info("After recovering {}, next Event ID to be generated will be {}", partitionDirectory, updatedId);
    }


    @Override
    public StorageResult addEvents(final Iterable<ProvenanceEventRecord> events) throws IOException {
        if (closed) {
            throw new IOException(this + " is closed");
        }

        // Claim a Record Writer Lease so that we have a writer to persist the events to
        boolean claimed = false;
        RecordWriterLease lease = null;
        while (!claimed) {
            lease = getLease();
            claimed = lease.tryClaim();

            if (claimed) {
                break;
            }

            if (lease.shouldRoll()) {
                tryRollover(lease);
            }
        }

        // Add the events to the writer and ensure that we always
        // relinquish the claim that we've obtained on the writer
        Map<ProvenanceEventRecord, StorageSummary> storageMap;
        final RecordWriter writer = lease.getWriter();
        try {
            storageMap = addEvents(events, writer);
        } finally {
            lease.relinquishClaim();
        }

        // Roll over the writer if necessary
        Integer eventsRolledOver = null;
        final boolean shouldRoll = lease.shouldRoll();
        try {
            if (shouldRoll && tryRollover(lease)) {
                eventsRolledOver = writer.getRecordsWritten();
            }
        } catch (final IOException ioe) {
            logger.error("Updated {} but failed to rollover to a new Event File", this, ioe);
        }

        final Integer rolloverCount = eventsRolledOver;
        return new StorageResult() {
            @Override
            public Map<ProvenanceEventRecord, StorageSummary> getStorageLocations() {
                return storageMap;
            }

            @Override
            public boolean triggeredRollover() {
                return rolloverCount != null;
            }

            @Override
            public Integer getEventsRolledOver() {
                return rolloverCount;
            }

            @Override
            public String toString() {
                return getStorageLocations().toString();
            }
        };
    }

    private RecordWriterLease getLease() throws IOException {
        while (true) {
            final RecordWriterLease lease = eventWriterLeaseRef.get();
            if (lease != null) {
                return lease;
            }

            if (tryRollover(null)) {
                return eventWriterLeaseRef.get();
            }
        }
    }

    private synchronized boolean tryRollover(final RecordWriterLease lease) throws IOException {
        if (!Objects.equals(lease, eventWriterLeaseRef.get())) {
            return false;
        }

        final long nextEventId = idGenerator.get();
        final File updatedEventFile = new File(partitionDirectory, nextEventId + ".prov");
        final RecordWriter updatedWriter = recordWriterFactory.createWriter(updatedEventFile, idGenerator, false, true);

        // Synchronize on the writer to ensure that no other thread is able to obtain the writer and start writing events to it until after it has
        // been fully initialized (i.e., the header has been written, etc.)
        synchronized (updatedWriter) {
            final RecordWriterLease updatedLease = new RecordWriterLease(updatedWriter, config.getMaxEventFileCapacity(), config.getMaxEventFileCount());
            final boolean updated = eventWriterLeaseRef.compareAndSet(lease, updatedLease);

            if (updated) {
                if (lease != null) {
                    lease.close();
                }

                updatedWriter.writeHeader(nextEventId);

                synchronized (minEventIdToPathMap) {
                    minEventIdToPathMap.put(nextEventId, updatedEventFile);
                }

                if (config.isCompressOnRollover() && lease != null && lease.getWriter() != null) {
                    boolean offered = false;
                    while (!offered && !closed) {
                        try {
                            offered = filesToCompress.offer(lease.getWriter().getFile(), 1, TimeUnit.SECONDS);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while waiting to enqueue " + lease.getWriter().getFile() + " for compression");
                        }
                    }
                }

                return true;
            } else {
                try {
                    updatedWriter.close();
                } catch (final Exception e) {
                    logger.warn("Failed to close Record Writer {}; some resources may not be cleaned up properly.", updatedWriter, e);
                }

                updatedEventFile.delete();
                return false;
            }
        }
    }

    private Map<ProvenanceEventRecord, StorageSummary> addEvents(final Iterable<ProvenanceEventRecord> events, final RecordWriter writer) throws IOException {
        final Map<ProvenanceEventRecord, StorageSummary> locationMap = new HashMap<>();

        try {
            long maxId = -1L;
            int numEvents = 0;
            for (final ProvenanceEventRecord nextEvent : events) {
                final StorageSummary writerSummary = writer.writeRecord(nextEvent);
                final StorageSummary summaryWithIndex = new StorageSummary(writerSummary.getEventId(), writerSummary.getStorageLocation(), this.partitionName,
                    writerSummary.getBlockIndex(), writerSummary.getSerializedLength(), writerSummary.getBytesWritten());
                locationMap.put(nextEvent, summaryWithIndex);
                maxId = summaryWithIndex.getEventId();
                numEvents++;
            }

            if (numEvents == 0) {
                return locationMap;
            }

            writer.flush();

            // Update max event id to be equal to be the greater of the current value or the
            // max value just written.
            final long maxIdWritten = maxId;
            this.maxEventId.getAndUpdate(cur -> maxIdWritten > cur ? maxIdWritten : cur);

            if (config.isAlwaysSync()) {
                writer.sync();
            }
        } catch (final Exception e) {
            // We need to set the repoDirty flag before we release the lock for this journal.
            // Otherwise, another thread may write to this journal -- this is a problem because
            // the journal contains part of our record but not all of it. Writing to the end of this
            // journal will result in corruption!
            writer.markDirty();
            throw e;
        }

        return locationMap;
    }


    @Override
    public long getSize() {
        return getEventFilesFromDisk()
            .collect(Collectors.summarizingLong(file -> file.length()))
            .getSum();
    }

    private Stream<File> getEventFilesFromDisk() {
        final File[] files = partitionDirectory.listFiles(DirectoryUtils.EVENT_FILE_FILTER);
        return files == null ? Stream.empty() : Arrays.stream(files);
    }

    @Override
    public long getMaxEventId() {
        return maxEventId.get();
    }

    @Override
    public Optional<ProvenanceEventRecord> getEvent(final long id) throws IOException {
        final Optional<File> option = getPathForEventId(id);
        if (!option.isPresent()) {
            return Optional.empty();
        }

        try (final RecordReader reader = recordReaderFactory.newRecordReader(option.get(), Collections.emptyList(), config.getMaxAttributeChars())) {
            final Optional<ProvenanceEventRecord> eventOption = reader.skipToEvent(id);
            if (!eventOption.isPresent()) {
                return eventOption;
            }

            // If an event is returned, the event may be the one we want, or it may be an event with a
            // higher event ID, if the desired event is not in the record reader. So we need to get the
            // event and check the Event ID to know whether to return the empty optional or the Optional
            // that was returned.
            final ProvenanceEventRecord event = eventOption.get();
            if (event.getEventId() == id) {
                return eventOption;
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxEvents, final EventAuthorizer authorizer) throws IOException {
        final List<ProvenanceEventRecord> events = new ArrayList<>(Math.min(maxEvents, 1000));
        try (final EventIterator iterator = createEventIterator(firstRecordId)) {
            Optional<ProvenanceEventRecord> eventOption;
            while ((eventOption = iterator.nextEvent()).isPresent() && events.size() < maxEvents) {
                final ProvenanceEventRecord event = eventOption.get();
                if (authorizer.isAuthorized(event)) {
                    events.add(event);
                }
            }
        }

        return events;
    }

    @Override
    public EventIterator createEventIterator(final long minDesiredId) {
        final List<File> filesOfInterest = new ArrayList<>();
        synchronized (minEventIdToPathMap) {
            File lastFile = null;

            for (final Map.Entry<Long, File> entry : minEventIdToPathMap.entrySet()) {
                final long minFileId = entry.getKey();

                // If the minimum ID for the file is greater than the minDesiredId, then
                // that means that we will want to iterate over this file.
                if (minFileId > minDesiredId) {
                    // The minimum ID for this file is greater than the desired ID, so
                    // that means that the last file we saw may have the minimum desired
                    // ID and any number of more events before we get to this file. So
                    // if we've not already added the lastFile, add it now.
                    if (filesOfInterest.isEmpty() && lastFile != null) {
                        filesOfInterest.add(lastFile);
                    }

                    filesOfInterest.add(entry.getValue());
                }

                lastFile = entry.getValue();
            }

            // We don't know the max ID of the last file, so we always want to include it, since it may contain
            // an event with an ID greater than minDesiredId.
            if (lastFile != null && !filesOfInterest.contains(lastFile)) {
                filesOfInterest.add(lastFile);
            }
        }

        if (filesOfInterest.isEmpty()) {
            return EventIterator.EMPTY;
        }

        return new SequentialRecordReaderEventIterator(filesOfInterest, recordReaderFactory, minDesiredId, config.getMaxAttributeChars());
    }


    @Override
    public EventIterator createEventIterator(final List<Long> eventIds) {
        final List<File> allFiles;
        synchronized (minEventIdToPathMap) {
            allFiles = new ArrayList<>(minEventIdToPathMap.values());
        }

        if (allFiles.isEmpty()) {
            return EventIterator.EMPTY;
        }

        return new SelectiveRecordReaderEventIterator(allFiles, recordReaderFactory, eventIds, config.getMaxAttributeChars());
    }

    private Optional<File> getPathForEventId(final long id) {
        File lastFile = null;

        synchronized (minEventIdToPathMap) {
            for (final Map.Entry<Long, File> entry : minEventIdToPathMap.entrySet()) {
                final long minId = entry.getKey();
                if (minId > id) {
                    break;
                }

                lastFile = entry.getValue();
            }
        }

        return Optional.ofNullable(lastFile);
    }


    @Override
    public void purgeOldEvents(final long olderThan, final TimeUnit unit) {
        final long timeCutoff = System.currentTimeMillis() - unit.toMillis(olderThan);

        getEventFilesFromDisk().filter(file -> file.lastModified() < timeCutoff)
            .sorted(DirectoryUtils.SMALLEST_ID_FIRST)
            .forEach(file -> delete(file));
    }


    @Override
    public long purgeOldestEvents() {
        final List<File> eventFiles = getEventFilesFromDisk().sorted(DirectoryUtils.SMALLEST_ID_FIRST).collect(Collectors.toList());
        if (eventFiles.isEmpty()) {
            return 0L;
        }

        for (final File eventFile : eventFiles) {
            final long fileSize = eventFile.length();

            if (delete(eventFile)) {
                logger.debug("{} Deleted {} event file ({}) due to storage limits", this, eventFile, FormatUtils.formatDataSize(fileSize));
                return fileSize;
            } else {
                logger.warn("{} Failed to delete oldest event file {}. This file should be cleaned up manually.", this, eventFile);
                continue;
            }
        }

        return 0L;
    }

    private boolean delete(final File file) {
        final long firstEventId = DirectoryUtils.getMinId(file);
        synchronized (minEventIdToPathMap) {
            minEventIdToPathMap.remove(firstEventId);
        }

        if (!file.delete()) {
            logger.warn("Failed to remove Provenance Event file {}; this file should be cleaned up manually", file);
            return false;
        }

        final File tocFile = TocUtil.getTocFile(file);
        if (tocFile.exists() && !tocFile.delete()) {
            logger.warn("Failed to remove Provenance Table-of-Contents file {}; this file should be cleaned up manually", tocFile);
        }

        return true;
    }

    void reindexLatestEvents(final EventIndex eventIndex) {
        final List<File> eventFiles = getEventFilesFromDisk().sorted(DirectoryUtils.SMALLEST_ID_FIRST).collect(Collectors.toList());
        if (eventFiles.isEmpty()) {
            return;
        }

        final long minEventIdToReindex = eventIndex.getMinimumEventIdToReindex(partitionName);
        final long maxEventId = getMaxEventId();
        final long eventsToReindex = maxEventId - minEventIdToReindex;

        logger.info("The last Provenance Event indexed for partition {} is {}, but the last event written to partition has ID {}. "
            + "Re-indexing up to the last {} events to ensure that the Event Index is accurate and up-to-date",
            partitionName, minEventIdToReindex, maxEventId, eventsToReindex, partitionDirectory);

        // Find the first event file that we care about.
        int firstEventFileIndex = 0;
        for (int i = eventFiles.size() - 1; i >= 0; i--) {
            final File eventFile = eventFiles.get(i);
            final long minIdInFile = DirectoryUtils.getMinId(eventFile);
            if (minIdInFile <= minEventIdToReindex) {
                firstEventFileIndex = i;
                break;
            }
        }

        // Create a subList that contains the files of interest
        final List<File> eventFilesToReindex = eventFiles.subList(firstEventFileIndex, eventFiles.size());

        final ExecutorService executor = Executors.newFixedThreadPool(Math.min(4, eventFilesToReindex.size()), new NamedThreadFactory("Re-Index Provenance Events", true));
        final List<Future<?>> futures = new ArrayList<>(eventFilesToReindex.size());
        final AtomicLong reindexedCount = new AtomicLong(0L);

        // Re-Index the last bunch of events.
        // We don't use an Event Iterator here because it's possible that one of the event files could be corrupt (for example, if NiFi does while
        // writing to the file, a record may be incomplete). We don't want to prevent us from moving on and continuing to index the rest of the
        // un-indexed events. So we just use a List of files and create a reader for each one.
        final long start = System.nanoTime();
        int fileCount = 0;
        for (final File eventFile : eventFilesToReindex) {
            final boolean skipToEvent;
            if (fileCount++ == 0) {
                skipToEvent = true;
            } else {
                skipToEvent = false;
            }

            final Runnable reindexTask = new Runnable() {
                @Override
                public void run() {
                    final Map<ProvenanceEventRecord, StorageSummary> storageMap = new HashMap<>(1000);

                    try (final RecordReader recordReader = recordReaderFactory.newRecordReader(eventFile, Collections.emptyList(), Integer.MAX_VALUE)) {
                        if (skipToEvent) {
                            final Optional<ProvenanceEventRecord> eventOption = recordReader.skipToEvent(minEventIdToReindex);
                            if (!eventOption.isPresent()) {
                                return;
                            }
                        }

                        StandardProvenanceEventRecord event = null;
                        while (true) {
                            final long startBytesConsumed = recordReader.getBytesConsumed();

                            event = recordReader.nextRecord();
                            if (event == null) {
                                eventIndex.reindexEvents(storageMap);
                                reindexedCount.addAndGet(storageMap.size());
                                storageMap.clear();
                                break; // stop reading from this file
                            } else {
                                final long eventSize = recordReader.getBytesConsumed() - startBytesConsumed;
                                storageMap.put(event, new StorageSummary(event.getEventId(), eventFile.getName(), partitionName, recordReader.getBlockIndex(), eventSize, 0L));

                                if (storageMap.size() == 1000) {
                                    eventIndex.reindexEvents(storageMap);
                                    reindexedCount.addAndGet(storageMap.size());
                                    storageMap.clear();
                                }
                            }
                        }
                    } catch (final EOFException eof) {
                        // Ran out of data. Continue on.
                        logger.warn("Failed to find event with ID {} in Event File {} due to {}", minEventIdToReindex, eventFile, eof.toString());
                    } catch (final Exception e) {
                        logger.error("Failed to index Provenance Events found in {}", eventFile, e);
                    }
                }
            };

            futures.add(executor.submit(reindexTask));
        }

        for (final Future<?> future : futures) {
            try {
                future.get();
            } catch (final ExecutionException ee) {
                logger.error("Failed to re-index some Provenance events. These events may not be query-able via the Provenance interface", ee.getCause());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for Provenance events to be re-indexed", e);
                break;
            }
        }

        try {
            eventIndex.commitChanges(partitionName);
        } catch (final IOException e) {
            logger.error("Failed to re-index Provenance Events for partition " + partitionName, e);
        }

        executor.shutdown();

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        final long seconds = millis / 1000L;
        final long millisRemainder = millis % 1000L;
        logger.info("Finished re-indexing {} events across {} files for {} in {}.{} seconds",
            reindexedCount.get(), eventFilesToReindex.size(), partitionDirectory, seconds, millisRemainder);
    }

    @Override
    public String toString() {
        return "Provenance Event Store Partition[directory=" + partitionDirectory + "]";
    }
}
