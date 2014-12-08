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
package org.apache.nifi.provenance;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.expiration.ExpirationAction;
import org.apache.nifi.provenance.expiration.FileRemovalAction;
import org.apache.nifi.provenance.lineage.FlowFileLineage;
import org.apache.nifi.provenance.lineage.Lineage;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.lucene.DeleteIndexAction;
import org.apache.nifi.provenance.lucene.FieldNames;
import org.apache.nifi.provenance.lucene.IndexSearch;
import org.apache.nifi.provenance.lucene.IndexingAction;
import org.apache.nifi.provenance.lucene.LineageQuery;
import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.provenance.rollover.CompressionAction;
import org.apache.nifi.provenance.rollover.RolloverAction;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriters;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.StopWatch;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentProvenanceRepository implements ProvenanceEventRepository {

    public static final String DEPRECATED_CLASS_NAME = "nifi.controller.repository.provenance.PersistentProvenanceRepository";
    public static final String EVENT_CATEGORY = "Provenance Repository";
    private static final String FILE_EXTENSION = ".prov";
    private static final String TEMP_FILE_SUFFIX = ".prov.part";
    public static final int SERIALIZATION_VERSION = 7;
    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");
    public static final Pattern INDEX_PATTERN = Pattern.compile("index-\\d+");
    public static final Pattern LOG_FILENAME_PATTERN = Pattern.compile("(\\d+).*\\.prov");
    public static final int MAX_UNDELETED_QUERY_RESULTS = 10;

    private static final Logger logger = LoggerFactory.getLogger(PersistentProvenanceRepository.class);

    private final long maxPartitionMillis;
    private final long maxPartitionBytes;

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final AtomicReference<SortedMap<Long, Path>> idToPathMap = new AtomicReference<>();
    private final AtomicBoolean recoveryFinished = new AtomicBoolean(false);

    private volatile boolean closed = false;

    // the following are all protected by the lock
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock writeLock = rwLock.writeLock();
    private final Lock readLock = rwLock.readLock();

    private RecordWriter[] writers; // guarded by readLock/writeLock

    private final AtomicLong streamStartTime = new AtomicLong(System.currentTimeMillis());
    private final RepositoryConfiguration configuration;
    private final IndexConfiguration indexConfig;
    private final boolean alwaysSync;

    private final ScheduledExecutorService scheduledExecService;
    private final ExecutorService rolloverExecutor;
    private final ExecutorService queryExecService;

    private final List<RolloverAction> rolloverActions = new ArrayList<>();
    private final List<ExpirationAction> expirationActions = new ArrayList<>();

    private final IndexingAction indexingAction;
    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();

    private final AtomicLong writerIndex = new AtomicLong(0L);
    private final AtomicLong storageDirectoryIndex = new AtomicLong(0L);
    private final AtomicLong bytesWrittenSinceRollover = new AtomicLong(0L);
    private final AtomicInteger recordsWrittenSinceRollover = new AtomicInteger(0);
    private final AtomicInteger rolloverCompletions = new AtomicInteger(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final AtomicBoolean repoDirty = new AtomicBoolean(false);
    // we keep the last 1000 records on hand so that when the UI is opened and it asks for the last 1000 records we don't need to 
    // read them. Since this is a very cheap operation to keep them, it's worth the tiny expense for the improved user experience.
    private final RingBuffer<ProvenanceEventRecord> latestRecords = new RingBuffer<>(1000);
    private EventReporter eventReporter;

    public PersistentProvenanceRepository() throws IOException {
        this(createRepositoryConfiguration());
    }

    public PersistentProvenanceRepository(final RepositoryConfiguration configuration) throws IOException {
        if (configuration.getStorageDirectories().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one storage directory");
        }

        this.configuration = configuration;

        for (final File file : configuration.getStorageDirectories()) {
            final Path storageDirectory = file.toPath();
            final Path journalDirectory = storageDirectory.resolve("journals");

            if (!Files.exists(journalDirectory)) {
                Files.createDirectories(journalDirectory);
            } else if (!Files.isDirectory(journalDirectory)) {
                throw new IllegalArgumentException("Storage Location " + journalDirectory + " is not a directory");
            }
        }

        this.maxPartitionMillis = configuration.getMaxEventFileLife(TimeUnit.MILLISECONDS);
        this.maxPartitionBytes = configuration.getMaxEventFileCapacity();
        this.indexConfig = new IndexConfiguration(configuration);
        this.alwaysSync = configuration.isAlwaysSync();

        final List<SearchableField> fields = configuration.getSearchableFields();
        if (fields != null && !fields.isEmpty()) {
            indexingAction = new IndexingAction(this, indexConfig);
            rolloverActions.add(indexingAction);
        } else {
            indexingAction = null;
        }

        if (configuration.isCompressOnRollover()) {
            rolloverActions.add(new CompressionAction());
        }

        scheduledExecService = Executors.newScheduledThreadPool(3);
        queryExecService = Executors.newFixedThreadPool(configuration.getQueryThreadPoolSize(), new NamedThreadFactory("Provenance Query Thread"));

        // The number of rollover threads is a little bit arbitrary but comes from the idea that multiple storage directories generally
        // live on separate physical partitions. As a result, we want to use at least one thread per partition in order to utilize the
        // disks efficiently. However, the rollover actions can be somewhat CPU intensive, so we double the number of threads in order
        // to account for that.
        final int numRolloverThreads = configuration.getStorageDirectories().size() * 2;
        rolloverExecutor = Executors.newFixedThreadPool(numRolloverThreads, new NamedThreadFactory("Provenance Repository Rollover Thread"));
    }

    @Override
    public void initialize(final EventReporter eventReporter) throws IOException {
        if (initialized.getAndSet(true)) {
            return;
        }

        this.eventReporter = eventReporter;

        recover();

        if (configuration.isAllowRollover()) {
            writers = createWriters(configuration, idGenerator.get());
        }

        if (configuration.isAllowRollover()) {
            scheduledExecService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    // Check if we need to roll over
                    if (needToRollover()) {
                        // it appears that we do need to roll over. Obtain write lock so that we can do so, and then
                        // confirm that we still need to.
                        writeLock.lock();
                        try {
                            logger.debug("Obtained write lock to perform periodic rollover");

                            if (needToRollover()) {
                                try {
                                    rollover(false);
                                } catch (final Exception e) {
                                    logger.error("Failed to roll over Provenance Event Log due to {}", e.toString());
                                    logger.error("", e);
                                }
                            }
                        } finally {
                            writeLock.unlock();
                        }
                    }
                }
            }, 10L, 10L, TimeUnit.SECONDS);

            scheduledExecService.scheduleWithFixedDelay(new RemoveExpiredQueryResults(), 30L, 3L, TimeUnit.SECONDS);
            scheduledExecService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        purgeOldEvents();
                    } catch (final Exception e) {
                        logger.error("Failed to purge old events from Provenance Repo due to {}", e.toString());
                        if (logger.isDebugEnabled()) {
                            logger.error("", e);
                        }
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to purge old events from Provenance Repo due to " + e.toString());
                    }
                }
            }, 1L, 1L, TimeUnit.MINUTES);

            expirationActions.add(new DeleteIndexAction(this, indexConfig));
            expirationActions.add(new FileRemovalAction());
        }
    }

    private static RepositoryConfiguration createRepositoryConfiguration() throws IOException {
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

        final RepositoryConfiguration config = new RepositoryConfiguration();
        for (final Path path : storageDirectories.values()) {
            config.addStorageDirectory(path.toFile());
        }
        config.setCompressOnRollover(compressOnRollover);
        config.setSearchableFields(searchableFields);
        config.setSearchableAttributes(searchableAttributes);
        config.setMaxEventFileCapacity(rolloverBytes);
        config.setMaxEventFileLife(rolloverMillis, TimeUnit.MILLISECONDS);
        config.setMaxRecordLife(storageMillis, TimeUnit.MILLISECONDS);
        config.setMaxStorageCapacity(maxStorageBytes);
        config.setQueryThreadPoolSize(queryThreads);
        config.setJournalCount(journalCount);

        if (shardSize != null) {
            config.setDesiredIndexSize(DataUnit.parseDataSize(shardSize, DataUnit.B).longValue());
        }

        config.setAlwaysSync(alwaysSync);

        return config;
    }

    static RecordWriter[] createWriters(final RepositoryConfiguration config, final long initialRecordId) throws IOException {
        final List<File> storageDirectories = config.getStorageDirectories();

        final RecordWriter[] writers = new RecordWriter[config.getJournalCount()];
        for (int i = 0; i < config.getJournalCount(); i++) {
            final File storageDirectory = storageDirectories.get(i % storageDirectories.size());
            final File journalDirectory = new File(storageDirectory, "journals");
            final File journalFile = new File(journalDirectory, String.valueOf(initialRecordId) + ".journal." + i);

            writers[i] = RecordWriters.newRecordWriter(journalFile);
            writers[i].writeHeader();
        }

        return writers;
    }

    @Override
    public StandardProvenanceEventRecord.Builder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
        persistRecord(Collections.singleton(event));
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
        persistRecord(events);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        final List<ProvenanceEventRecord> records = new ArrayList<>(maxRecords);

        final List<Path> paths = getPathsForId(firstRecordId);
        if (paths == null || paths.isEmpty()) {
            return records;
        }

        for (final Path path : paths) {
            try (RecordReader reader = RecordReaders.newRecordReader(path.toFile(), getAllLogFiles())) {
                StandardProvenanceEventRecord record;
                while (records.size() < maxRecords && ((record = reader.nextRecord()) != null)) {
                    if (record.getEventId() >= firstRecordId) {
                        records.add(record);
                    }
                }
            } catch (final EOFException | FileNotFoundException fnfe) {
                // assume file aged off (or there's no data in file, in case of EOFException, which indicates that data was cached
                // in operating system and entire O/S crashed and always.sync was not turned on.)
            } catch (final IOException ioe) {
                logger.error("Failed to read Provenance Event File {} due to {}", path.toFile(), ioe.toString());
                logger.error("", ioe);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to read Provenance Event File " + path.toFile() + " due to " + ioe.toString());
            }

            if (records.size() >= maxRecords) {
                break;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving up to {} records starting at Event ID {}; returning {} events", maxRecords, firstRecordId, records.size());
        }

        return records;
    }

    private List<Path> getPathsForId(final long id) {
        final SortedMap<Long, Path> map = idToPathMap.get();

        final List<Path> paths = new ArrayList<>();

        final Iterator<Map.Entry<Long, Path>> itr = map.entrySet().iterator();
        if (!itr.hasNext()) {
            return paths;
        }

        Map.Entry<Long, Path> lastEntry = itr.next();
        while (itr.hasNext()) {
            final Map.Entry<Long, Path> entry = itr.next();
            final Long startIndex = entry.getKey();

            if (startIndex >= id) {
                paths.add(lastEntry.getValue());
                paths.add(entry.getValue());

                while (itr.hasNext()) {
                    paths.add(itr.next().getValue());
                }

                return paths;
            }

            lastEntry = entry;
        }

        // we didn't find any entry whose first ID is greater than the id
        // requested. However,
        // since we don't know the max ID of the last entry in the map, it's
        // possible that the
        // ID that we want lives within that file, so we add it to the paths to
        // return
        if (lastEntry != null) {
            paths.add(lastEntry.getValue());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Looking for Event ID {}, searching in paths: {}", id, paths);
        }

        return paths;
    }

    public RepositoryConfiguration getConfiguration() {
        return configuration;
    }

    private void recover() throws IOException {
        long maxId = -1L;
        long maxIndexedId = -1L;
        long minIndexedId = Long.MAX_VALUE;

        final List<File> filesToRecover = new ArrayList<>();
        for (final File file : configuration.getStorageDirectories()) {
            final File[] matchingFiles = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(final File pathname) {
                    final String filename = pathname.getName();
                    if (!filename.contains(FILE_EXTENSION) || filename.endsWith(TEMP_FILE_SUFFIX)) {
                        return false;
                    }

                    final String baseFilename = filename.substring(0, filename.indexOf("."));
                    return NUMBER_PATTERN.matcher(baseFilename).matches();
                }
            });
            for (final File matchingFile : matchingFiles) {
                filesToRecover.add(matchingFile);
            }
        }

        final SortedMap<Long, Path> sortedPathMap = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(final Long o1, final Long o2) {
                return Long.compare(o1, o2);
            }
        });

        File maxIdFile = null;
        for (final File file : filesToRecover) {
            final String filename = file.getName();
            final String baseName = filename.substring(0, filename.indexOf("."));
            final long firstId = Long.parseLong(baseName);
            sortedPathMap.put(firstId, file.toPath());

            if (firstId > maxId) {
                maxId = firstId;
                maxIdFile = file;
            }

            if (firstId > maxIndexedId && indexingAction != null && indexingAction.hasBeenPerformed(file)) {
                maxIndexedId = firstId - 1;
            }

            if (firstId < minIndexedId && indexingAction != null && indexingAction.hasBeenPerformed(file)) {
                minIndexedId = firstId;
            }
        }

        if (maxIdFile != null) {
            final boolean lastFileIndexed = indexingAction == null ? false : indexingAction.hasBeenPerformed(maxIdFile);

            // Determine the max ID in the last file.
            try (final RecordReader reader = RecordReaders.newRecordReader(maxIdFile, getAllLogFiles())) {
                ProvenanceEventRecord record;
                while ((record = reader.nextRecord()) != null) {
                    final long eventId = record.getEventId();
                    if (eventId > maxId) {
                        maxId = eventId;
                    }

                    // If the ID is greater than the max indexed id and this file was indexed, then
                    // update the max indexed id
                    if (eventId > maxIndexedId && lastFileIndexed) {
                        maxIndexedId = eventId;
                    }
                }
            } catch (final IOException ioe) {
                logger.error("Failed to read Provenance Event File {} due to {}", maxIdFile, ioe);
                logger.error("", ioe);
            }
        }

        if (maxIndexedId > -1L) {
            // If we have indexed anything then set the min/max ID's indexed.
            indexConfig.setMaxIdIndexed(maxIndexedId);
        }

        if (minIndexedId < Long.MAX_VALUE) {
            indexConfig.setMinIdIndexed(minIndexedId);
        }

        idGenerator.set(maxId + 1);

        // TODO: Consider refactoring this so that we do the rollover actions at the same time that we merge the journals.
        // This would require a few different changes:
        //      * Rollover Actions would take a ProvenanceEventRecord at a time, not a File at a time. Would have to be either
        //          constructed or "started" for each file and then closed after each file
        //      * The recovery would have to then read through all of the journals with the highest starting ID to determine
        //          the action max id instead of reading the merged file
        //      * We would write to a temporary file and then rename once the merge is complete. This allows us to fail and restart
        try {
            final Set<File> recoveredJournals = recoverJournalFiles();
            filesToRecover.addAll(recoveredJournals);

            // Find the file that has the greatest ID
            File greatestMinIdFile = null;
            long greatestMinId = 0L;
            for (final File recoveredJournal : recoveredJournals) {
                // if the file was removed because the journals were empty, don't count it
                if (!recoveredJournal.exists()) {
                    continue;
                }

                final String basename = LuceneUtil.substringBefore(recoveredJournal.getName(), ".");
                try {
                    final long minId = Long.parseLong(basename);

                    sortedPathMap.put(minId, recoveredJournal.toPath());
                    if (greatestMinIdFile == null || minId > greatestMinId) {
                        greatestMinId = minId;
                        greatestMinIdFile = recoveredJournal;
                    }
                } catch (final NumberFormatException nfe) {
                    // not a file we care about...
                }
            }

            // Read the records in the last file to find its max id
            if (greatestMinIdFile != null) {
                try (final RecordReader recordReader = RecordReaders.newRecordReader(greatestMinIdFile, Collections.<Path>emptyList())) {
                    StandardProvenanceEventRecord record;

                    try {
                        while ((record = recordReader.nextRecord()) != null) {
                            if (record.getEventId() > maxId) {
                                maxId = record.getEventId();
                            }
                        }
                    } catch (final EOFException eof) {
                    }
                }
            }

            // set the ID Generator 1 greater than the max id
            idGenerator.set(maxId + 1);
        } catch (final IOException ioe) {
            logger.error("Failed to recover Journal Files due to {}", ioe.toString());
            logger.error("", ioe);
        }

        idToPathMap.set(Collections.unmodifiableSortedMap(sortedPathMap));
        logger.trace("In recovery, path map: {}", sortedPathMap);

        final long recordsRecovered;
        if (minIndexedId < Long.MAX_VALUE) {
            recordsRecovered = idGenerator.get() - minIndexedId;
        } else {
            recordsRecovered = idGenerator.get();
        }

        logger.info("Recovered {} records", recordsRecovered);

        final List<RolloverAction> rolloverActions = this.rolloverActions;
        final Runnable retroactiveRollover = new Runnable() {
            @Override
            public void run() {
                for (File toRecover : filesToRecover) {
                    final String baseFileName = LuceneUtil.substringBefore(toRecover.getName(), ".");
                    final Long fileFirstEventId = Long.parseLong(baseFileName);

                    for (final RolloverAction action : rolloverActions) {
                        if (!action.hasBeenPerformed(toRecover)) {
                            try {
                                final StopWatch stopWatch = new StopWatch(true);

                                toRecover = action.execute(toRecover);

                                stopWatch.stop();
                                final String duration = stopWatch.getDuration();
                                logger.info("Successfully performed retroactive action {} against {} in {}", action, toRecover, duration);

                                // update our map of id to Path
                                final Map<Long, Path> updatedMap = addToPathMap(fileFirstEventId, toRecover.toPath());
                                logger.trace("After retroactive rollover action {}, Path Map: {}", action, updatedMap);
                            } catch (final Exception e) {
                                logger.error("Failed to perform retroactive rollover actions on {} due to {}", toRecover, e.toString());
                                logger.error("", e);
                                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to perform retroactive rollover actions on " + toRecover + " due to " + e.toString());
                            }
                        }
                    }
                }
            }
        };
        rolloverExecutor.submit(retroactiveRollover);

        recoveryFinished.set(true);
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            logger.debug("Obtained write lock for close");

            this.closed = true;
            scheduledExecService.shutdownNow();
            rolloverExecutor.shutdownNow();
            queryExecService.shutdownNow();

            for (final RecordWriter writer : writers) {
                writer.close();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isShutdownComplete() {
        return this.closed;
    }

    private void persistRecord(final Iterable<ProvenanceEventRecord> records) {
        final long totalJournalSize;
        readLock.lock();
        try {
            if (repoDirty.get()) {
                logger.debug("Cannot persist provenance record because there was an IOException last time a record persistence was attempted. Will not attempt to persist more records until the repo has been rolled over.");
                return;
            }

            final RecordWriter[] recordWriters = this.writers;
            long bytesWritten = 0L;

            // obtain a lock on one of the RecordWriter's so that no other thread is able to write to this writer until we're finished.
            // Although the writer itself is thread-safe, we need to generate an event id and then write the event
            // atomically, so we need to do this with a lock.
            boolean locked = false;
            RecordWriter writer;
            do {
                final long idx = writerIndex.getAndIncrement();
                writer = recordWriters[(int) (idx % recordWriters.length)];
                locked = writer.tryLock();
            } while (!locked);

            try {
                try {
                    for (final ProvenanceEventRecord nextRecord : records) {
                        final long eventId = idGenerator.getAndIncrement();
                        bytesWritten += writer.writeRecord(nextRecord, eventId);
                        logger.trace("Wrote record with ID {} to {}", eventId, writer);
                    }

                    if (alwaysSync) {
                        writer.sync();
                    }

                    totalJournalSize = bytesWrittenSinceRollover.addAndGet(bytesWritten);
                    recordsWrittenSinceRollover.getAndIncrement();
                } catch (final IOException ioe) {
                    // We need to set the repoDirty flag before we release the lock for this journal.
                    // Otherwise, another thread may write to this journal -- this is a problem because
                    // the journal contains part of our record but not all of it. Writing to the end of this
                    // journal will result in corruption!
                    repoDirty.set(true);
                    streamStartTime.set(0L);    // force rollover to happen soon.
                    throw ioe;
                } finally {
                    writer.unlock();
                }
            } catch (final IOException ioe) {
                logger.error("Failed to persist Provenance Event due to {}. Will not attempt to write to the Provenance Repository again until the repository has rolled over.", ioe.toString());
                logger.error("", ioe);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to persist Provenance Event due to " + ioe.toString() + ". Will not attempt to write to the Provenance Repository again until the repository has rolled over");

                // Switch from readLock to writeLock so that we can perform rollover
                readLock.unlock();
                try {
                    writeLock.lock();
                    try {
                        logger.debug("Obtained write lock to rollover due to IOException on write");
                        rollover(true);
                    } finally {
                        writeLock.unlock();
                    }
                } catch (final Exception e) {
                    logger.error("Failed to Rollover Provenance Event Repository file due to {}", e.toString());
                    logger.error("", e);
                    eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to Rollover Provenance Event Repository file due to " + e.toString());
                } finally {
                    readLock.lock();
                }

                return;
            }
        } finally {
            readLock.unlock();
        }

        // If the total number of bytes written to the Journals is >= configured max, we need to roll over
        if (totalJournalSize >= configuration.getMaxEventFileCapacity()) {
            writeLock.lock();
            try {
                logger.debug("Obtained write lock to perform rollover based on file size");

                // now that we've obtained the lock, we need to verify that we still need to do the rollover, as
                // another thread may have just done it.
                if (bytesWrittenSinceRollover.get() >= configuration.getMaxEventFileCapacity()) {
                    try {
                        rollover(false);
                    } catch (IOException e) {
                        logger.error("Failed to Rollover Provenance Event Repository file due to {}", e.toString());
                        logger.error("", e);
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to Rollover Provenance Event Repository file due to " + e.toString());
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    private List<File> getLogFiles() {
        final List<File> files = new ArrayList<>();
        for (final Path path : idToPathMap.get().values()) {
            files.add(path.toFile());
        }

        if (files.isEmpty()) {
            return files;
        }

        return files;
    }

    /**
     * Returns the size, in bytes, of the Repository storage
     *
     * @param logFiles
     * @param timeCutoff
     * @return
     */
    public long getSize(final List<File> logFiles, final long timeCutoff) {
        long bytesUsed = 0L;

        // calculate the size of the repository
        for (final File file : logFiles) {
            final long lastModified = file.lastModified();
            if (lastModified > 0L && lastModified < timeCutoff) {
                continue;
            }

            bytesUsed += file.length();
        }

        // take into account the size of the indices
        bytesUsed += indexConfig.getIndexSize();
        return bytesUsed;
    }

    /**
     * Purges old events from the repository
     *
     * @throws IOException
     */
    void purgeOldEvents() throws IOException {
        while (!recoveryFinished.get()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }

        final List<File> toPurge = new ArrayList<>();
        final long timeCutoff = System.currentTimeMillis() - configuration.getMaxRecordLife(TimeUnit.MILLISECONDS);

        final List<File> sortedByBasename = getLogFiles();
        long bytesUsed = getSize(sortedByBasename, timeCutoff);

        for (final Path path : idToPathMap.get().values()) {
            final File file = path.toFile();
            final long lastModified = file.lastModified();
            if (lastModified > 0L && lastModified < timeCutoff) {
                toPurge.add(file);
            }
        }

        final Comparator<File> sortByBasenameComparator = new Comparator<File>() {
            @Override
            public int compare(final File o1, final File o2) {
                final String baseName1 = LuceneUtil.substringBefore(o1.getName(), ".");
                final String baseName2 = LuceneUtil.substringBefore(o2.getName(), ".");

                Long id1 = null;
                Long id2 = null;
                try {
                    id1 = Long.parseLong(baseName1);
                } catch (final NumberFormatException nfe) {
                    id1 = null;
                }

                try {
                    id2 = Long.parseLong(baseName2);
                } catch (final NumberFormatException nfe) {
                    id2 = null;
                }

                if (id1 == null && id2 == null) {
                    return 0;
                }
                if (id1 == null) {
                    return 1;
                }
                if (id2 == null) {
                    return -1;
                }

                return Long.compare(id1, id2);
            }
        };

        // If we have too much data, start aging it off
        if (bytesUsed > configuration.getMaxStorageCapacity()) {
            Collections.sort(sortedByBasename, sortByBasenameComparator);

            for (final File file : sortedByBasename) {
                toPurge.add(file);
                bytesUsed -= file.length();
                if (bytesUsed < configuration.getMaxStorageCapacity()) {
                    // we've shrunk the repo size down enough to stop
                    break;
                }
            }
        }

        // Sort all of the files that we want to purge such that the oldest events are aged off first
        Collections.sort(toPurge, sortByBasenameComparator);
        logger.debug("Purging old event files: {}", toPurge);

        // Remove any duplicates that we may have.
        final Set<File> uniqueFilesToPurge = new LinkedHashSet<>(toPurge);

        // Age off the data.
        final Set<String> removed = new LinkedHashSet<>();
        for (File file : uniqueFilesToPurge) {
            final String baseName = LuceneUtil.substringBefore(file.getName(), ".");
            ExpirationAction currentAction = null;
            try {
                for (final ExpirationAction action : expirationActions) {
                    currentAction = action;
                    if (!action.hasBeenPerformed(file)) {
                        final File fileBeforeAction = file;
                        final StopWatch stopWatch = new StopWatch(true);
                        file = action.execute(file);
                        stopWatch.stop();
                        logger.info("Successfully performed Expiration Action {} on Provenance Event file {} in {}", action, fileBeforeAction, stopWatch.getDuration());
                    }
                }

                removed.add(baseName);
            } catch (final FileNotFoundException fnf) {
                logger.warn("Failed to perform Expiration Action {} on Provenance Event file {} because the file no longer exists; will not perform additional Expiration Actions on this file", currentAction, file);
                removed.add(baseName);
            } catch (final Throwable t) {
                logger.warn("Failed to perform Expiration Action {} on Provenance Event file {} due to {}; will not perform additional Expiration Actions on this file at this time", currentAction, file, t.toString());
                logger.warn("", t);
                eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to perform Expiration Action " + currentAction + " on Provenance Event file " + file + " due to " + t.toString() + "; will not perform additional Expiration Actions on this file at this time");
            }
        }

        // Update the Map ID to Path map to not include the removed file
        // we use a lock here because otherwise we have a check-then-modify between get/set on the AtomicReference.
        // But we keep the AtomicReference because in other places we do just a .get()
        writeLock.lock();
        try {
            logger.debug("Obtained write lock to update ID/Path Map for expiration");

            final SortedMap<Long, Path> pathMap = idToPathMap.get();
            final SortedMap<Long, Path> newPathMap = new TreeMap<>(new PathMapComparator());
            newPathMap.putAll(pathMap);
            final Iterator<Map.Entry<Long, Path>> itr = newPathMap.entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, Path> entry = itr.next();
                final String filename = entry.getValue().toFile().getName();
                final String baseName = LuceneUtil.substringBefore(filename, ".");

                if (removed.contains(baseName)) {
                    itr.remove();
                }
            }
            idToPathMap.set(newPathMap);
            logger.debug("After expiration, path map: {}", newPathMap);
        } finally {
            writeLock.unlock();
        }
    }

    public void waitForRollover() throws IOException {
        final int count = rolloverCompletions.get();
        while (rolloverCompletions.get() == count) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
            }
        }
    }

    /**
     * MUST be called with the write lock held
     *
     * @param force
     * @throws IOException
     */
    private void rollover(final boolean force) throws IOException {
        if (!configuration.isAllowRollover()) {
            return;
        }

        // If this is the first time we're creating the out stream, or if we
        // have written something to the stream, then roll over
        if (recordsWrittenSinceRollover.get() > 0L || repoDirty.get() || force) {
            final List<File> journalsToMerge = new ArrayList<>();
            for (final RecordWriter writer : writers) {
                final File writerFile = writer.getFile();
                journalsToMerge.add(writerFile);
                writer.close();
            }

            writers = createWriters(configuration, idGenerator.get());
            streamStartTime.set(System.currentTimeMillis());
            recordsWrittenSinceRollover.getAndSet(0);

            final long storageDirIdx = storageDirectoryIndex.getAndIncrement();
            final List<File> storageDirs = configuration.getStorageDirectories();
            final File storageDir = storageDirs.get((int) (storageDirIdx % storageDirs.size()));

            final List<RolloverAction> actions = rolloverActions;
            final int recordsWritten = recordsWrittenSinceRollover.getAndSet(0);
            final Runnable rolloverRunnable = new Runnable() {
                @Override
                public void run() {
                    final File fileRolledOver;

                    try {
                        fileRolledOver = mergeJournals(journalsToMerge, storageDir, getMergeFile(journalsToMerge, storageDir), eventReporter, latestRecords);
                        repoDirty.set(false);
                    } catch (final IOException ioe) {
                        repoDirty.set(true);
                        logger.error("Failed to merge Journal Files {} into a Provenance Log File due to {}", journalsToMerge, ioe.toString());
                        logger.error("", ioe);
                        return;
                    }

                    if (fileRolledOver == null) {
                        return;
                    }
                    File file = fileRolledOver;

                    for (final RolloverAction action : actions) {
                        try {
                            final StopWatch stopWatch = new StopWatch(true);
                            file = action.execute(file);
                            stopWatch.stop();
                            logger.info("Successfully performed Rollover Action {} for {} in {}", action, file, stopWatch.getDuration());

                            // update our map of id to Path
                            // need lock to update the map, even though it's an AtomicReference, AtomicReference allows those doing a
                            // get() to obtain the most up-to-date version but we use a writeLock to prevent multiple threads modifying
                            // it at one time
                            writeLock.lock();
                            try {
                                final Long fileFirstEventId = Long.valueOf(LuceneUtil.substringBefore(fileRolledOver.getName(), "."));
                                SortedMap<Long, Path> newIdToPathMap = new TreeMap<>(new PathMapComparator());
                                newIdToPathMap.putAll(idToPathMap.get());
                                newIdToPathMap.put(fileFirstEventId, file.toPath());
                                idToPathMap.set(newIdToPathMap);
                                logger.trace("After rollover action {}, path map: {}", action, newIdToPathMap);
                            } finally {
                                writeLock.unlock();
                            }
                        } catch (final Throwable t) {
                            logger.error("Failed to perform Rollover Action {} for {}: got Exception {}",
                                    action, fileRolledOver, t.toString());
                            logger.error("", t);

                            return;
                        }
                    }

                    if (actions.isEmpty()) {
                        // update our map of id to Path
                        // need lock to update the map, even though it's an AtomicReference, AtomicReference allows those doing a
                        // get() to obtain the most up-to-date version but we use a writeLock to prevent multiple threads modifying
                        // it at one time
                        writeLock.lock();
                        try {
                            final Long fileFirstEventId = Long.valueOf(LuceneUtil.substringBefore(fileRolledOver.getName(), "."));
                            SortedMap<Long, Path> newIdToPathMap = new TreeMap<>(new PathMapComparator());
                            newIdToPathMap.putAll(idToPathMap.get());
                            newIdToPathMap.put(fileFirstEventId, file.toPath());
                            idToPathMap.set(newIdToPathMap);
                        } finally {
                            writeLock.unlock();
                        }
                    }

                    logger.info("Successfully Rolled over Provenance Event file containing {} records", recordsWritten);
                    rolloverCompletions.getAndIncrement();
                }
            };

            rolloverExecutor.submit(rolloverRunnable);

            streamStartTime.set(System.currentTimeMillis());
            bytesWrittenSinceRollover.set(0);
        }
    }

    private SortedMap<Long, Path> addToPathMap(final Long firstEventId, final Path path) {
        SortedMap<Long, Path> unmodifiableMap;
        boolean updated = false;
        do {
            final SortedMap<Long, Path> existingMap = idToPathMap.get();
            final SortedMap<Long, Path> newIdToPathMap = new TreeMap<>(new PathMapComparator());
            newIdToPathMap.putAll(existingMap);
            newIdToPathMap.put(firstEventId, path);
            unmodifiableMap = Collections.unmodifiableSortedMap(newIdToPathMap);

            updated = idToPathMap.compareAndSet(existingMap, unmodifiableMap);
        } while (!updated);

        return unmodifiableMap;
    }

    private Set<File> recoverJournalFiles() throws IOException {
        if (!configuration.isAllowRollover()) {
            return Collections.emptySet();
        }

        final Map<String, List<File>> journalMap = new HashMap<>();

        // Map journals' basenames to the files with that basename.
        final List<File> storageDirs = configuration.getStorageDirectories();
        for (final File storageDir : configuration.getStorageDirectories()) {
            final File journalDir = new File(storageDir, "journals");
            if (!journalDir.exists()) {
                continue;
            }

            final File[] journalFiles = journalDir.listFiles();
            if (journalFiles == null) {
                continue;
            }

            for (final File journalFile : journalFiles) {
                final String basename = LuceneUtil.substringBefore(journalFile.getName(), ".");
                List<File> files = journalMap.get(basename);
                if (files == null) {
                    files = new ArrayList<>();
                    journalMap.put(basename, files);
                }

                files.add(journalFile);
            }
        }

        final Set<File> mergedFiles = new HashSet<>();
        for (final List<File> journalFileSet : journalMap.values()) {
            final long storageDirIdx = storageDirectoryIndex.getAndIncrement();
            final File storageDir = storageDirs.get((int) (storageDirIdx % storageDirs.size()));
            final File mergedFile = mergeJournals(journalFileSet, storageDir, getMergeFile(journalFileSet, storageDir), eventReporter, latestRecords);
            if (mergedFile != null) {
                mergedFiles.add(mergedFile);
            }
        }

        return mergedFiles;
    }

    static File getMergeFile(final List<File> journalFiles, final File storageDir) {
        // verify that all Journal files have the same basename
        String canonicalBaseName = null;
        for (final File journal : journalFiles) {
            final String basename = LuceneUtil.substringBefore(journal.getName(), ".");
            if (canonicalBaseName == null) {
                canonicalBaseName = basename;
            }

            if (!canonicalBaseName.equals(basename)) {
                throw new IllegalArgumentException("Cannot merge journal files because they do not contain the same basename, which means that they are not correlated properly");
            }
        }

        final File mergedFile = new File(storageDir, canonicalBaseName + ".prov");
        return mergedFile;
    }

    static File mergeJournals(final List<File> journalFiles, final File storageDir, final File mergedFile, final EventReporter eventReporter, final RingBuffer<ProvenanceEventRecord> ringBuffer) throws IOException {
        final long startNanos = System.nanoTime();
        if (journalFiles.isEmpty()) {
            return null;
        }

        if (mergedFile.exists()) {
            throw new FileAlreadyExistsException("Cannot Merge " + journalFiles.size() + " Journal Files into Merged Provenance Log File " + mergedFile.getAbsolutePath() + " because the Merged File already exists");
        }

        final File tempMergedFile = new File(mergedFile.getParentFile(), mergedFile.getName() + ".part");

        // Map each journal to a RecordReader
        final List<RecordReader> readers = new ArrayList<>();
        int records = 0;

        try {
            for (final File journalFile : journalFiles) {
                try {
                    readers.add(RecordReaders.newRecordReader(journalFile, null));
                } catch (final EOFException eof) {
                    // there's nothing here. Skip over it.
                } catch (final IOException ioe) {
                    logger.warn("Unable to merge {} with other Journal Files due to {}", journalFile, ioe.toString());
                    if (logger.isDebugEnabled()) {
                        logger.warn("", ioe);
                    }

                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to merge Journal Files due to " + ioe.toString());
                    }
                }
            }

            // Create a Map so that the key is the next record available from a reader and the value is the Reader from which
            // the record came. This sorted map is then used so that we are able to always get the first entry, which is the next
            // lowest record id
            final SortedMap<StandardProvenanceEventRecord, RecordReader> recordToReaderMap = new TreeMap<>(new Comparator<StandardProvenanceEventRecord>() {
                @Override
                public int compare(final StandardProvenanceEventRecord o1, final StandardProvenanceEventRecord o2) {
                    return Long.compare(o1.getEventId(), o2.getEventId());
                }
            });

            for (final RecordReader reader : readers) {
                StandardProvenanceEventRecord record = null;

                try {
                    record = reader.nextRecord();
                } catch (final EOFException eof) {
                } catch (final Exception e) {
                    logger.warn("Failed to generate Provenance Event Record from Journal due to " + e + "; it's possible that the record wasn't completely written to the file. This record will be skipped.");
                    if (logger.isDebugEnabled()) {
                        logger.warn("", e);
                    }

                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to read Provenance Event Record from Journal due to " + e + "; it's possible that hte record wasn't completely written to the file. This record will be skipped.");
                }

                if (record == null) {
                    continue;
                }

                recordToReaderMap.put(record, reader);
            }

            // loop over each entry in the map, persisting the records to the merged file in order, and populating the map
            // with the next entry from the journal file from which the previous record was written.
            try (final RecordWriter writer = RecordWriters.newRecordWriter(tempMergedFile)) {
                writer.writeHeader();

                while (!recordToReaderMap.isEmpty()) {
                    final Map.Entry<StandardProvenanceEventRecord, RecordReader> entry = recordToReaderMap.entrySet().iterator().next();
                    final StandardProvenanceEventRecord record = entry.getKey();
                    final RecordReader reader = entry.getValue();

                    writer.writeRecord(record, record.getEventId());
                    ringBuffer.add(record);
                    records++;

                    // Remove this entry from the map
                    recordToReaderMap.remove(record);

                    // Get the next entry from this reader and add it to the map
                    StandardProvenanceEventRecord nextRecord = null;

                    try {
                        nextRecord = reader.nextRecord();
                    } catch (final EOFException eof) {
                    }

                    if (nextRecord != null) {
                        recordToReaderMap.put(nextRecord, reader);
                    }
                }
            }
        } finally {
            for (final RecordReader reader : readers) {
                try {
                    reader.close();
                } catch (final IOException ioe) {
                }
            }
        }

        // Attempt to rename. Keep trying for a bit if we fail. This happens often if we have some external process
        // that locks files, such as a virus scanner.
        boolean renamed = false;
        for (int i = 0; i < 10 && !renamed; i++) {
            renamed = tempMergedFile.renameTo(mergedFile);
            if (!renamed) {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException ie) {
                }
            }
        }

        if (!renamed) {
            throw new IOException("Failed to merge journal files into single merged file " + mergedFile.getAbsolutePath() + " because " + tempMergedFile.getAbsolutePath() + " could not be renamed");
        }

        // Success. Remove all of the journal files, as they're no longer needed, now that they've been merged.
        for (final File journalFile : journalFiles) {
            if (!journalFile.delete()) {
                if (journalFile.exists()) {
                    logger.warn("Failed to remove temporary journal file {}; this file should be cleaned up manually", journalFile.getAbsolutePath());
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to remove temporary journal file " + journalFile.getAbsolutePath() + "; this file should be cleaned up manually");
                } else {
                    logger.warn("Failed to remove temporary journal file {} because it no longer exists", journalFile.getAbsolutePath());
                }
            }
        }

        if (records == 0) {
            mergedFile.delete();
            return null;
        } else {
            final long nanos = System.nanoTime() - startNanos;
            final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
            logger.info("Successfully merged {} journal files ({} records) into single Provenance Log File {} in {} milliseconds", journalFiles.size(), records, mergedFile, millis);
        }

        return mergedFile;
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        final List<SearchableField> searchableFields = new ArrayList<>(configuration.getSearchableFields());
        // we exclude the Event Time because it is always searchable and is a bit special in its handling
        // because it dictates in some cases which index files we look at
        searchableFields.remove(SearchableFields.EventTime);
        return searchableFields;
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return new ArrayList<>(configuration.getSearchableAttributes());
    }

    QueryResult queryEvents(final Query query) throws IOException {
        final QuerySubmission submission = submitQuery(query);
        final QueryResult result = submission.getResult();
        while (!result.isFinished()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }

        if (result.getError() != null) {
            throw new IOException(result.getError());
        }
        logger.info("{} got {} hits", query, result.getTotalHitCount());
        return result;
    }

    @Override
    public QuerySubmission submitQuery(final Query query) {
        final int numQueries = querySubmissionMap.size();
        if (numQueries > MAX_UNDELETED_QUERY_RESULTS) {
            throw new IllegalStateException("Cannot process query because there are currently " + numQueries + " queries whose results have not been deleted due to poorly behaving clients not issuing DELETE requests. Please try again later.");
        }

        if (query.getEndDate() != null && query.getStartDate() != null && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }

        if (query.getSearchTerms().isEmpty() && query.getStartDate() == null && query.getEndDate() == null) {
            final AsyncQuerySubmission result = new AsyncQuerySubmission(query, 1);

            if (latestRecords.getSize() >= query.getMaxResults()) {
                final List<ProvenanceEventRecord> latestList = latestRecords.asList();
                final List<ProvenanceEventRecord> trimmed;
                if (latestList.size() > query.getMaxResults()) {
                    trimmed = latestList.subList(latestList.size() - query.getMaxResults(), latestList.size());
                } else {
                    trimmed = latestList;
                }

                final Long maxEventId = getMaxEventId();
                if (maxEventId == null) {
                    result.getResult().update(Collections.<ProvenanceEventRecord>emptyList(), 0L);
                }
                Long minIndexedId = indexConfig.getMinIdIndexed();
                if (minIndexedId == null) {
                    minIndexedId = 0L;
                }

                final long totalNumDocs = maxEventId - minIndexedId;

                result.getResult().update(trimmed, totalNumDocs);
            } else {
                queryExecService.submit(new GetMostRecentRunnable(query, result));
            }

            querySubmissionMap.put(query.getIdentifier(), result);
            return result;
        }

        final AtomicInteger retrievalCount = new AtomicInteger(0);
        final List<File> indexDirectories = indexConfig.getIndexDirectories(
                query.getStartDate() == null ? null : query.getStartDate().getTime(),
                query.getEndDate() == null ? null : query.getEndDate().getTime());
        final AsyncQuerySubmission result = new AsyncQuerySubmission(query, indexDirectories.size());
        querySubmissionMap.put(query.getIdentifier(), result);

        if (indexDirectories.isEmpty()) {
            result.getResult().update(Collections.<ProvenanceEventRecord>emptyList(), 0L);
        } else {
            for (final File indexDir : indexDirectories) {
                queryExecService.submit(new QueryRunnable(query, result, indexDir, retrievalCount));
            }
        }

        return result;
    }

    /**
     * REMOVE-ME: This is for testing only and can be removed.
     *
     * @param luceneQuery
     * @return
     * @throws IOException
     */
    public Iterator<ProvenanceEventRecord> queryLucene(final org.apache.lucene.search.Query luceneQuery) throws IOException {
        final List<File> indexFiles = indexConfig.getIndexDirectories();

        final AtomicLong hits = new AtomicLong(0L);
        final List<Future<List<Document>>> futures = new ArrayList<>();
        for (final File indexDirectory : indexFiles) {
            final Callable<List<Document>> callable = new Callable<List<Document>>() {
                @Override
                public List<Document> call() {
                    final List<Document> localScoreDocs = new ArrayList<>();

                    try (final DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(indexDirectory))) {
                        final IndexSearcher searcher = new IndexSearcher(directoryReader);

                        TopDocs topDocs = searcher.search(luceneQuery, 10000000);
                        logger.info("For {}, Top Docs has {} hits; reading Lucene results", indexDirectory, topDocs.scoreDocs.length);

                        if (topDocs.totalHits > 0) {
                            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                                final int docId = scoreDoc.doc;
                                final Document d = directoryReader.document(docId);
                                localScoreDocs.add(d);
                            }
                        }

                        hits.addAndGet(localScoreDocs.size());
                    } catch (final IndexNotFoundException e) {
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }

                    return localScoreDocs;
                }
            };

            final Future<List<Document>> future = queryExecService.submit(callable);
            futures.add(future);
        }

        logger.info("Merging results of Lucene query ({} hits)", hits.get());
        List<Document> scoreDocs = null;
        int idx = 0;
        for (final Future<List<Document>> future : futures) {
            try {
                final List<Document> docs = future.get();
                if (idx++ == 0) {
                    scoreDocs = docs;
                } else {
                    scoreDocs.addAll(docs);
                    docs.clear();
                }
            } catch (final ExecutionException | InterruptedException ee) {
                throw new RuntimeException(ee);
            }
        }

        logger.info("Finished querying Lucene; there are {} docs; sorting for retrieval", scoreDocs.size());

        LuceneUtil.sortDocsForRetrieval(scoreDocs);
        logger.info("Finished sorting for retrieval. Returning Iterator.");

        final Iterator<Document> docItr = scoreDocs.iterator();
        final Collection<Path> allLogFiles = getAllLogFiles();
        return new Iterator<ProvenanceEventRecord>() {
            int count = 0;
            RecordReader reader = null;
            String lastStorageFilename = null;
            long lastByteOffset = 0L;

            @Override
            public boolean hasNext() {
                return docItr.hasNext();
            }

            @Override
            public ProvenanceEventRecord next() {
                if (count++ > 0) {
                    // remove last document so that we don't hold everything in memory.
                    docItr.remove();
                }

                final Document doc = docItr.next();

                final String storageFilename = doc.getField(FieldNames.STORAGE_FILENAME).stringValue();
                final long byteOffset = doc.getField(FieldNames.STORAGE_FILE_OFFSET).numericValue().longValue();

                try {
                    if (reader != null && storageFilename.equals(lastStorageFilename) && byteOffset > lastByteOffset) {
                        // Still the same file and the offset is downstream.
                        try {
                            reader.skipTo(byteOffset);
                            final StandardProvenanceEventRecord record = reader.nextRecord();
                            return record;
                        } catch (final IOException e) {
                            if (hasNext()) {
                                return next();
                            } else {
                                return null;
                            }
                        }

                    } else {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (final IOException ioe) {
                            }
                        }

                        final List<File> potentialFiles = LuceneUtil.getProvenanceLogFiles(storageFilename, allLogFiles);
                        if (potentialFiles.isEmpty()) {
                            if (hasNext()) {
                                return next();
                            } else {
                                return null;
                            }
                        }

                        if (potentialFiles.size() > 1) {
                            if (hasNext()) {
                                return next();
                            } else {
                                return null;
                            }
                        }

                        for (final File file : potentialFiles) {
                            try {
                                reader = RecordReaders.newRecordReader(file, allLogFiles);
                            } catch (final IOException ioe) {
                                continue;
                            }

                            try {
                                reader.skip(byteOffset);

                                final StandardProvenanceEventRecord record = reader.nextRecord();
                                return record;
                            } catch (final IOException e) {
                                continue;
                            }
                        }
                    }
                } finally {
                    lastStorageFilename = storageFilename;
                    lastByteOffset = byteOffset;
                }

                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    Lineage computeLineage(final String flowFileUuid) throws IOException {
        return computeLineage(Collections.<String>singleton(flowFileUuid), LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }

    private Lineage computeLineage(final Collection<String> flowFileUuids, final LineageComputationType computationType, final Long eventId, final Long startTimestamp, final Long endTimestamp) throws IOException {
        final AsyncLineageSubmission submission = submitLineageComputation(flowFileUuids, computationType, eventId, startTimestamp, endTimestamp);
        final StandardLineageResult result = submission.getResult();
        while (!result.isFinished()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }

        if (result.getError() != null) {
            throw new IOException(result.getError());
        }

        return new FlowFileLineage(result.getNodes(), result.getEdges());
    }

    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid) {
        return submitLineageComputation(Collections.singleton(flowFileUuid), LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }

    private AsyncLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids, final LineageComputationType computationType, final Long eventId, final long startTimestamp, final long endTimestamp) {
        final List<File> indexDirs = indexConfig.getIndexDirectories(startTimestamp, endTimestamp);
        final AsyncLineageSubmission result = new AsyncLineageSubmission(computationType, eventId, flowFileUuids, indexDirs.size());
        lineageSubmissionMap.put(result.getLineageIdentifier(), result);

        for (final File indexDir : indexDirs) {
            queryExecService.submit(new ComputeLineageRunnable(flowFileUuids, result, indexDir));
        }

        return result;
    }

    @Override
    public AsyncLineageSubmission submitExpandChildren(final long eventId) {
        try {
            final ProvenanceEventRecord event = getEvent(eventId);
            if (event == null) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.<ProvenanceEventRecord>emptyList());
                return submission;
            }

            switch (event.getEventType()) {
                case CLONE:
                case FORK:
                case JOIN:
                case REPLAY:
                    return submitLineageComputation(event.getChildUuids(), LineageComputationType.EXPAND_CHILDREN, eventId, event.getEventTime(), Long.MAX_VALUE);
                default:
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1);
                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its children cannot be expanded");
                    return submission;
            }
        } catch (final IOException ioe) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);

            if (ioe.getMessage() == null) {
                submission.getResult().setError(ioe.toString());
            } else {
                submission.getResult().setError(ioe.getMessage());
            }

            return submission;
        }
    }

    @Override
    public AsyncLineageSubmission submitExpandParents(final long eventId) {
        try {
            final ProvenanceEventRecord event = getEvent(eventId);
            if (event == null) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.<ProvenanceEventRecord>emptyList());
                return submission;
            }

            switch (event.getEventType()) {
                case JOIN:
                case FORK:
                case CLONE:
                case REPLAY:
                    return submitLineageComputation(event.getParentUuids(), LineageComputationType.EXPAND_PARENTS, eventId, 0L, event.getEventTime());
                default: {
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.<String>emptyList(), 1);
                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its parents cannot be expanded");
                    return submission;
                }
            }
        } catch (final IOException ioe) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.<String>emptyList(), 1);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);

            if (ioe.getMessage() == null) {
                submission.getResult().setError(ioe.toString());
            } else {
                submission.getResult().setError(ioe.getMessage());
            }

            return submission;
        }
    }

    @Override
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier) {
        return lineageSubmissionMap.get(lineageIdentifier);
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier) {
        return querySubmissionMap.get(queryIdentifier);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) throws IOException {
        final List<ProvenanceEventRecord> records = getEvents(id, 1);
        if (records.isEmpty()) {
            return null;
        }
        final ProvenanceEventRecord record = records.get(0);
        if (record.getEventId() != id) {
            return null;
        }
        return record;
    }

    private boolean needToRollover() {
        final long writtenSinceRollover = bytesWrittenSinceRollover.get();

        if (writtenSinceRollover >= maxPartitionBytes) {
            return true;
        }

        if (repoDirty.get() || (writtenSinceRollover > 0 && System.currentTimeMillis() > streamStartTime.get() + maxPartitionMillis)) {
            return true;
        }

        return false;
    }

    public Collection<Path> getAllLogFiles() {
        final SortedMap<Long, Path> map = idToPathMap.get();
        return (map == null) ? new ArrayList<Path>() : map.values();
    }

    private static class PathMapComparator implements Comparator<Long> {

        @Override
        public int compare(final Long o1, final Long o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return 1;
            }
            if (o2 == null) {
                return -1;
            }
            return Long.compare(o1, o2);
        }
    }

    @Override
    public Long getMaxEventId() {
        return indexConfig.getMaxIdIndexed();
    }

    private class GetMostRecentRunnable implements Runnable {

        private final Query query;
        private final AsyncQuerySubmission submission;

        public GetMostRecentRunnable(final Query query, final AsyncQuerySubmission submission) {
            this.query = query;
            this.submission = submission;
        }

        @Override
        public void run() {
            // get the max indexed event id
            final Long maxEventId = indexConfig.getMaxIdIndexed();
            if (maxEventId == null) {
                submission.getResult().update(Collections.<ProvenanceEventRecord>emptyList(), 0);
                return;
            }

            final int maxResults = query.getMaxResults();
            final long startIndex = Math.max(maxEventId - query.getMaxResults(), 0L);

            try {
                Long minIndexedId = indexConfig.getMinIdIndexed();
                if (minIndexedId == null) {
                    minIndexedId = 0L;
                }
                final long totalNumDocs = maxEventId - minIndexedId;

                final List<ProvenanceEventRecord> mostRecent = getEvents(startIndex, maxResults);
                submission.getResult().update(mostRecent, totalNumDocs);
            } catch (final IOException ioe) {
                logger.error("Failed to retrieve records from Provenance Repository: " + ioe.toString());
                if (logger.isDebugEnabled()) {
                    logger.error("", ioe);
                }

                if (ioe.getMessage() == null) {
                    submission.getResult().setError("Failed to retrieve records from Provenance Repository: " + ioe.toString());
                } else {
                    submission.getResult().setError("Failed to retrieve records from Provenance Repository: " + ioe.getMessage());
                }
            }
        }
    }

    private class QueryRunnable implements Runnable {

        private final Query query;
        private final AsyncQuerySubmission submission;
        private final File indexDir;
        private final AtomicInteger retrievalCount;

        public QueryRunnable(final Query query, final AsyncQuerySubmission submission, final File indexDir, final AtomicInteger retrievalCount) {
            this.query = query;
            this.submission = submission;
            this.indexDir = indexDir;
            this.retrievalCount = retrievalCount;
        }

        @Override
        public void run() {
            try {
                final IndexSearch search = new IndexSearch(PersistentProvenanceRepository.this, indexDir);
                final StandardQueryResult queryResult = search.search(query, retrievalCount);
                submission.getResult().update(queryResult.getMatchingEvents(), queryResult.getTotalHitCount());
                if (queryResult.isFinished()) {
                    logger.info("Successfully executed Query[{}] against Index {}; Search took {} milliseconds; Total Hits = {}",
                            query, indexDir, queryResult.getQueryTime(), queryResult.getTotalHitCount());
                }
            } catch (final Throwable t) {
                logger.error("Failed to query provenance repository due to {}", t.toString());
                if (logger.isDebugEnabled()) {
                    logger.error("", t);
                }

                if (t.getMessage() == null) {
                    submission.getResult().setError(t.toString());
                } else {
                    submission.getResult().setError(t.getMessage());
                }
            }
        }
    }

    private class ComputeLineageRunnable implements Runnable {

        private final Collection<String> flowFileUuids;
        private final File indexDir;
        private final AsyncLineageSubmission submission;

        public ComputeLineageRunnable(final Collection<String> flowFileUuids, final AsyncLineageSubmission submission, final File indexDir) {
            this.flowFileUuids = flowFileUuids;
            this.submission = submission;
            this.indexDir = indexDir;
        }

        @Override
        public void run() {
            if (submission.isCanceled()) {
                return;
            }

            try {
                final Set<ProvenanceEventRecord> matchingRecords = LineageQuery.computeLineageForFlowFiles(PersistentProvenanceRepository.this, indexDir, null, flowFileUuids);
                final StandardLineageResult result = submission.getResult();
                result.update(matchingRecords);

                logger.info("Successfully created Lineage for FlowFiles with UUIDs {} in {} milliseconds; Lineage contains {} nodes and {} edges",
                        flowFileUuids, result.getComputationTime(TimeUnit.MILLISECONDS), result.getNodes().size(), result.getEdges().size());
            } catch (final Throwable t) {
                logger.error("Failed to query provenance repository due to {}", t.toString());
                if (logger.isDebugEnabled()) {
                    logger.error("", t);
                }

                if (t.getMessage() == null) {
                    submission.getResult().setError(t.toString());
                } else {
                    submission.getResult().setError(t.getMessage());
                }
            }
        }
    }

    private class RemoveExpiredQueryResults implements Runnable {

        @Override
        public void run() {
            try {
                final Date now = new Date();

                final Iterator<Map.Entry<String, AsyncQuerySubmission>> queryIterator = querySubmissionMap.entrySet().iterator();
                while (queryIterator.hasNext()) {
                    final Map.Entry<String, AsyncQuerySubmission> entry = queryIterator.next();

                    final StandardQueryResult result = entry.getValue().getResult();
                    if (entry.getValue().isCanceled() || (result.isFinished() && result.getExpiration().before(now))) {
                        queryIterator.remove();
                    }
                }

                final Iterator<Map.Entry<String, AsyncLineageSubmission>> lineageIterator = lineageSubmissionMap.entrySet().iterator();
                while (lineageIterator.hasNext()) {
                    final Map.Entry<String, AsyncLineageSubmission> entry = lineageIterator.next();

                    final StandardLineageResult result = entry.getValue().getResult();
                    if (entry.getValue().isCanceled() || (result.isFinished() && result.getExpiration().before(now))) {
                        lineageIterator.remove();
                    }
                }
            } catch (final Throwable t) {
                logger.error("Failed to expire Provenance Query Results due to {}", t.toString());
                logger.error("", t);
            }
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(0);
        private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        private final String namePrefix;

        public NamedThreadFactory(final String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(final Runnable r) {
            final Thread thread = defaultThreadFactory.newThread(r);
            thread.setName(namePrefix + "-" + counter.incrementAndGet());
            return thread;
        }
    }
}
