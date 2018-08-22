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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.expiration.ExpirationAction;
import org.apache.nifi.provenance.expiration.FileRemovalAction;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.FlowFileLineage;
import org.apache.nifi.provenance.lineage.Lineage;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.lucene.DeleteIndexAction;
import org.apache.nifi.provenance.lucene.DocsReader;
import org.apache.nifi.provenance.lucene.DocumentToEventConverter;
import org.apache.nifi.provenance.lucene.FieldNames;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.lucene.IndexSearch;
import org.apache.nifi.provenance.lucene.IndexingAction;
import org.apache.nifi.provenance.lucene.LineageQuery;
import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.provenance.lucene.SimpleIndexManager;
import org.apache.nifi.provenance.lucene.UpdateMinimumEventId;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriters;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.util.NamedThreadFactory;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.RingBuffer.ForEachEvaluator;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.timebuffer.CountSizeEntityAccess;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimedCountSize;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.apache.nifi.web.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
import java.util.stream.Collectors;

/**
 * @deprecated This class is now deprecated in favor of {@link WriteAheadProvenanceRepository}.
 */
@Deprecated
public class PersistentProvenanceRepository implements ProvenanceRepository {

    public static final String EVENT_CATEGORY = "Provenance Repository";
    private static final String FILE_EXTENSION = ".prov";
    private static final String TEMP_FILE_SUFFIX = ".prov.part";
    private static final long PURGE_EVENT_MILLISECONDS = 2500L; //Determines the frequency over which the task to delete old events will occur
    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");
    public static final Pattern INDEX_PATTERN = Pattern.compile("index-\\d+");
    public static final Pattern LOG_FILENAME_PATTERN = Pattern.compile("(\\d+).*\\.prov");
    public static final int MAX_UNDELETED_QUERY_RESULTS = 10;
    public static final int MAX_INDEXING_FAILURE_COUNT = 5; // how many indexing failures we will tolerate before skipping indexing for a prov file
    public static final int MAX_JOURNAL_ROLLOVER_RETRIES = 5;

    private static final float PURGE_OLD_EVENTS_HIGH_WATER = 0.9f;
    private static final float PURGE_OLD_EVENTS_LOW_WATER = 0.88f;
    private static final float ROLLOVER_HIGH_WATER = 0.99f;

    private static final Logger logger = LoggerFactory.getLogger(PersistentProvenanceRepository.class);

    private final long maxPartitionMillis;
    private final long maxPartitionBytes;

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final AtomicReference<SortedMap<Long, Path>> idToPathMap = new AtomicReference<>();
    private final AtomicBoolean recoveryFinished = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile long firstEventTimestamp = 0L;

    // the following are all protected by the lock
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock writeLock = rwLock.writeLock();
    private final Lock readLock = rwLock.readLock();

    private RecordWriter[] writers; // guarded by readLock/writeLock

    private final AtomicLong streamStartTime = new AtomicLong(System.currentTimeMillis());
    private final RepositoryConfiguration configuration;
    private final IndexConfiguration indexConfig;
    private final IndexManager indexManager;
    private final boolean alwaysSync;
    private final int rolloverCheckMillis;
    private final int maxAttributeChars;

    private final ScheduledExecutorService scheduledExecService;
    private final ScheduledExecutorService rolloverExecutor;
    private final ExecutorService queryExecService;

    private final List<ExpirationAction> expirationActions = new ArrayList<>();

    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();

    private final AtomicLong writerIndex = new AtomicLong(0L);
    private final AtomicLong storageDirectoryIndex = new AtomicLong(0L);
    private final AtomicLong bytesWrittenSinceRollover = new AtomicLong(0L);
    private final AtomicInteger recordsWrittenSinceRollover = new AtomicInteger(0);
    private final AtomicInteger rolloverCompletions = new AtomicInteger(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final AtomicInteger dirtyWriterCount = new AtomicInteger(0);

    // we keep the last 1000 records on hand so that when the UI is opened and it asks for the last 1000 records we don't need to
    // read them. Since this is a very cheap operation to keep them, it's worth the tiny expense for the improved user experience.
    private final RingBuffer<ProvenanceEventRecord> latestRecords = new RingBuffer<>(1000);
    private EventReporter eventReporter; // effectively final
    private Authorizer authorizer;  // effectively final
    private ProvenanceAuthorizableFactory resourceFactory;  // effectively final

    private final TimedBuffer<TimedCountSize> updateCounts = new TimedBuffer<>(TimeUnit.SECONDS, 300, new CountSizeEntityAccess());
    private final TimedBuffer<TimestampedLong> backpressurePauseMillis = new TimedBuffer<>(TimeUnit.SECONDS, 300, new LongEntityAccess());

    /**
     * default no args constructor for service loading only.
     */
    public PersistentProvenanceRepository() {
        maxPartitionMillis = 0;
        maxPartitionBytes = 0;
        writers = null;
        configuration = null;
        indexConfig = null;
        indexManager = null;
        alwaysSync = false;
        rolloverCheckMillis = 0;
        maxAttributeChars = 0;
        scheduledExecService = null;
        rolloverExecutor = null;
        queryExecService = null;
        eventReporter = null;
        authorizer = null;
        resourceFactory = null;
    }

    public PersistentProvenanceRepository(final NiFiProperties nifiProperties) throws IOException {
        this(RepositoryConfiguration.create(nifiProperties), 10000);
    }

    public PersistentProvenanceRepository(final RepositoryConfiguration configuration, final int rolloverCheckMillis) throws IOException {
        if (configuration.getStorageDirectories().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one storage directory");
        }

        this.configuration = configuration;
        this.maxAttributeChars = configuration.getMaxAttributeChars();

        for (final File file : configuration.getStorageDirectories().values()) {
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
        this.indexManager = new SimpleIndexManager(configuration);
        this.alwaysSync = configuration.isAlwaysSync();
        this.rolloverCheckMillis = rolloverCheckMillis;

        scheduledExecService = Executors.newScheduledThreadPool(3, new NamedThreadFactory("Provenance Maintenance Thread"));
        queryExecService = Executors.newFixedThreadPool(configuration.getQueryThreadPoolSize(), new NamedThreadFactory("Provenance Query Thread"));

        // The number of rollover threads is a little bit arbitrary but comes from the idea that multiple storage directories generally
        // live on separate physical partitions. As a result, we want to use at least one thread per partition in order to utilize the
        // disks efficiently. However, the rollover actions can be somewhat CPU intensive, so we double the number of threads in order
        // to account for that.
        final int numRolloverThreads = configuration.getStorageDirectories().size() * 2;
        rolloverExecutor = Executors.newScheduledThreadPool(numRolloverThreads, new NamedThreadFactory("Provenance Repository Rollover Thread"));
    }

    protected IndexManager getIndexManager() {
        return indexManager;
    }

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory,
        final IdentifierLookup idLookup) throws IOException {
        writeLock.lock();
        try {
            if (initialized.getAndSet(true)) {
                return;
            }

            this.eventReporter = eventReporter;
            this.authorizer = authorizer;
            this.resourceFactory = resourceFactory;

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
                                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to roll over Provenance Event Log due to " + e.toString());
                                    }
                                }
                            } finally {
                                writeLock.unlock();
                            }
                        }
                    }
                }, rolloverCheckMillis, rolloverCheckMillis, TimeUnit.MILLISECONDS);

                expirationActions.add(new UpdateMinimumEventId(indexConfig));
                expirationActions.add(new FileRemovalAction());

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
                }, PURGE_EVENT_MILLISECONDS, PURGE_EVENT_MILLISECONDS, TimeUnit.MILLISECONDS);
            }

            firstEventTimestamp = determineFirstEventTimestamp();
        } finally {
            writeLock.unlock();
        }
    }


    // protected in order to override for unit tests
    protected RecordWriter[] createWriters(final RepositoryConfiguration config, final long initialRecordId) throws IOException {
        final List<File> storageDirectories = new ArrayList<>(config.getStorageDirectories().values());

        final RecordWriter[] writers = new RecordWriter[config.getJournalCount()];
        for (int i = 0; i < config.getJournalCount(); i++) {
            final File storageDirectory = storageDirectories.get(i % storageDirectories.size());
            final File journalDirectory = new File(storageDirectory, "journals");
            final File journalFile = new File(journalDirectory, String.valueOf(initialRecordId) + ".journal." + i);

            writers[i] = RecordWriters.newSchemaRecordWriter(journalFile, idGenerator, false, false);
            writers[i].writeHeader(initialRecordId);
        }

        logger.info("Created new Provenance Event Writers for events starting with ID {}", initialRecordId);
        return writers;
    }

    /**
     * @return the maximum number of characters that any Event attribute should
     * contain. If the event contains more characters than this, the attribute
     * may be truncated on retrieval
     */
    public int getMaxAttributeCharacters() {
        return maxAttributeChars;
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

    public boolean isAuthorized(final ProvenanceEventRecord event, final NiFiUser user) {
        if (authorizer == null || user == null) {
            return true;
        }

        final Authorizable eventAuthorizable;
        try {
            eventAuthorizable = resourceFactory.createProvenanceDataAuthorizable(event.getComponentId());
        } catch (final ResourceNotFoundException rnfe) {
            return false;
        }

        final AuthorizationResult result = eventAuthorizable.checkAuthorization(authorizer, RequestAction.READ, user);
        return Result.Approved.equals(result.getResult());
    }

    public void authorize(final ProvenanceEventRecord event, final NiFiUser user) {
        if (authorizer == null || user == null) {
            return;
        }

        final Authorizable eventAuthorizable = resourceFactory.createProvenanceDataAuthorizable(event.getComponentId());
        eventAuthorizable.authorize(authorizer, RequestAction.READ, user);
    }

    public List<ProvenanceEventRecord> filterUnauthorizedEvents(final List<ProvenanceEventRecord> events, final NiFiUser user) {
        return events.stream().filter(event -> isAuthorized(event, user)).collect(Collectors.<ProvenanceEventRecord>toList());
    }

    public Set<ProvenanceEventRecord> replaceUnauthorizedWithPlaceholders(final Set<ProvenanceEventRecord> events, final NiFiUser user) {
        return events.stream().map(event -> isAuthorized(event, user) ? event : new PlaceholderProvenanceEvent(event)).collect(Collectors.toSet());
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        return getEvents(firstRecordId, maxRecords, null);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final NiFiUser user) throws IOException {
        final List<ProvenanceEventRecord> records = new ArrayList<>(maxRecords);

        final List<Path> paths = getPathsForId(firstRecordId);
        if (paths == null || paths.isEmpty()) {
            return records;
        }

        for (final Path path : paths) {
            try (RecordReader reader = RecordReaders.newRecordReader(path.toFile(), getAllLogFiles(), maxAttributeChars)) {
                // if this is the first record, try to find out the block index and jump directly to
                // the block index. This avoids having to read through a lot of data that we don't care about
                // just to get to the first record that we want.
                if (records.isEmpty()) {
                    final TocReader tocReader = reader.getTocReader();
                    if (tocReader != null) {
                        final Integer blockIndex = tocReader.getBlockIndexForEventId(firstRecordId);
                        if (blockIndex != null) {
                            reader.skipToBlock(blockIndex);
                        }
                    }
                }

                StandardProvenanceEventRecord record;
                while (records.size() < maxRecords && (record = reader.nextRecord()) != null) {
                    if (record.getEventId() >= firstRecordId && isAuthorized(record, user)) {
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

    @Override
    public Set<String> getContainerNames() {
        return new HashSet<>(configuration.getStorageDirectories().keySet());
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        Map<String, File> map = configuration.getStorageDirectories();

        File container = map.get(containerName);
        if(container != null) {
            long capacity = FileUtils.getContainerCapacity(container.toPath());
            if(capacity==0) {
                throw new IOException("System returned total space of the partition for " + containerName + " is zero byte. "
                        + "Nifi can not create a zero sized provenance repository.");
            }
            return capacity;
        } else {
            throw new IllegalArgumentException("There is no defined container with name " + containerName);
        }
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        final Map<String, File> map = configuration.getStorageDirectories();
        final File container = map.get(containerName);
        if (container == null) {
            return null;
        }

        try {
            return Files.getFileStore(container.toPath()).name();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public long getContainerUsableSpace(String containerName) throws IOException {
        Map<String, File> map = configuration.getStorageDirectories();

        File container = map.get(containerName);
        if(container != null) {
            return FileUtils.getContainerUsableSpace(container.toPath());
        } else {
            throw new IllegalArgumentException("There is no defined container with name " + containerName);
        }
    }

    private void recover() throws IOException {
        long maxId = -1L;
        long maxIndexedId = -1L;
        long minIndexedId = Long.MAX_VALUE;

        final List<File> filesToRecover = new ArrayList<>();
        for (final File file : configuration.getStorageDirectories().values()) {
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

            if (firstId > maxIndexedId) {
                maxIndexedId = firstId - 1;
            }

            if (firstId < minIndexedId) {
                minIndexedId = firstId;
            }
        }

        if (maxIdFile != null) {
            // Determine the max ID in the last file.
            try (final RecordReader reader = RecordReaders.newRecordReader(maxIdFile, getAllLogFiles(), maxAttributeChars)) {
                final long eventId = reader.getMaxEventId();
                if (eventId > maxId) {
                    maxId = eventId;
                }

                // If the ID is greater than the max indexed id and this file was indexed, then
                // update the max indexed id
                if (eventId > maxIndexedId) {
                    maxIndexedId = eventId;
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
                try (final RecordReader recordReader = RecordReaders.newRecordReader(greatestMinIdFile, Collections.<Path>emptyList(), maxAttributeChars)) {
                    maxId = recordReader.getMaxEventId();
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
        recoveryFinished.set(true);
    }

    @Override
    public synchronized void close() throws IOException {
        this.closed.set(true);
        writeLock.lock();
        try {
            logger.debug("Obtained write lock for close");

            scheduledExecService.shutdownNow();
            rolloverExecutor.shutdownNow();
            queryExecService.shutdownNow();

            getIndexManager().close();

            if (writers != null) {
                for (final RecordWriter writer : writers) {
                    writer.close();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isShutdownComplete() {
        return this.closed.get();
    }

    private void persistRecord(final Iterable<ProvenanceEventRecord> records) {
        final long totalJournalSize;
        readLock.lock();
        try {
            long bytesWritten = 0L;

            // obtain a lock on one of the RecordWriter's so that no other thread is able to write to this writer until we're finished.
            // Although the writer itself is thread-safe, we need to generate an event id and then write the event
            // atomically, so we need to do this with a lock.
            boolean locked = false;
            RecordWriter writer;
            do {
                final RecordWriter[] recordWriters = this.writers;
                final int numDirty = dirtyWriterCount.get();
                if (numDirty >= recordWriters.length) {
                    throw new IllegalStateException("Cannot update repository because all partitions are unusable at this time. Writing to the repository would cause corruption. "
                            + "This most often happens as a result of the repository running out of disk space or the JVM running out of memory.");
                }

                final long idx = writerIndex.getAndIncrement();
                writer = recordWriters[(int) (idx % recordWriters.length)];
                locked = writer.tryLock();
            } while (!locked);

            try {
                try {
                    long recordsWritten = 0L;
                    for (final ProvenanceEventRecord nextRecord : records) {
                        final StorageSummary persistedEvent = writer.writeRecord(nextRecord);
                        bytesWritten += persistedEvent.getSerializedLength();
                        recordsWritten++;
                        logger.trace("Wrote record with ID {} to {}", persistedEvent.getEventId(), writer);
                    }

                    writer.flush();

                    if (alwaysSync) {
                        writer.sync();
                    }

                    totalJournalSize = bytesWrittenSinceRollover.addAndGet(bytesWritten);
                    recordsWrittenSinceRollover.getAndIncrement();
                    this.updateCounts.add(new TimedCountSize(recordsWritten, bytesWritten));
                } catch (final Throwable t) {
                    // We need to set the repoDirty flag before we release the lock for this journal.
                    // Otherwise, another thread may write to this journal -- this is a problem because
                    // the journal contains part of our record but not all of it. Writing to the end of this
                    // journal will result in corruption!
                    writer.markDirty();
                    dirtyWriterCount.incrementAndGet();
                    streamStartTime.set(0L);    // force rollover to happen soon.
                    throw t;
                } finally {
                    writer.unlock();
                }
            } catch (final IOException ioe) {
                // warn about the failure
                logger.error("Failed to persist Provenance Event due to {}.", ioe.toString());
                logger.error("", ioe);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to persist Provenance Event due to " + ioe.toString());

                // Attempt to perform a rollover. An IOException in this part of the code generally is the result of
                // running out of disk space. If we have multiple partitions, we may well be able to rollover. This helps
                // in two ways: it compresses the journal files which frees up space, and if it ends up merging to a different
                // partition/storage directory, we can delete the journals from this directory that ran out of space.
                // In order to do this, though, we must switch from a read lock to a write lock.
                // This part of the code gets a little bit messy, and we could potentially refactor it a bit in order to
                // make the code cleaner.
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
                    eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to Rollover Provenance Event Log due to " + e.toString());
                } finally {
                    // we must re-lock the readLock, as the finally block below is going to unlock it.
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
                    } catch (final IOException e) {
                        logger.error("Failed to Rollover Provenance Event Repository file due to {}", e.toString());
                        logger.error("", e);
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to Rollover Provenance Event Log due to " + e.toString());
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * @return all of the Provenance Event Log Files (not the journals, the
     * merged files) available across all storage directories.
     */
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
     * @param logFiles the log files to consider
     * @param timeCutoff if a log file's last modified date is before
     * timeCutoff, it will be skipped
     * @return the size of all log files given whose last mod date comes after
     * (or equal to) timeCutoff
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
     * @throws IOException if unable to purge old events due to an I/O problem
     */
    synchronized void purgeOldEvents() throws IOException {
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

        // This comparator sorts the data based on the "basename" of the files. I.e., the numeric portion.
        // We do this because the numeric portion represents the ID of the first event in the log file.
        // As a result, we are sorting based on time, since the ID is monotonically increasing. By doing this,
        // are able to avoid hitting disk continually to check timestamps
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

        // If we have too much data (at least 90% of our max capacity), start aging it off
        if (bytesUsed > configuration.getMaxStorageCapacity() * PURGE_OLD_EVENTS_HIGH_WATER) {
            Collections.sort(sortedByBasename, sortByBasenameComparator);

            for (final File file : sortedByBasename) {
                toPurge.add(file);
                bytesUsed -= file.length();
                if (bytesUsed < configuration.getMaxStorageCapacity() * PURGE_OLD_EVENTS_LOW_WATER) {
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
                logger.warn("Failed to perform Expiration Action {} on Provenance Event file {} because the file no longer exists; will not "
                        + "perform additional Expiration Actions on this file", currentAction, file);
                removed.add(baseName);
            } catch (final Throwable t) {
                logger.warn("Failed to perform Expiration Action {} on Provenance Event file {} due to {}; will not perform additional "
                        + "Expiration Actions on this file at this time", currentAction, file, t.toString());
                logger.warn("", t);
                eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to perform Expiration Action " + currentAction
                        + " on Provenance Event file " + file + " due to " + t.toString() + "; will not perform additional Expiration Actions "
                        + "on this file at this time");
            }
        }

        // Update the Map ID to Path map to not include the removed file
        // We cannot obtain the write lock here because there may be a need for the lock in the rollover method,
        // if we have 'backpressure applied'. This would result in a deadlock because the rollover method would be
        // waiting for purgeOldEvents, and purgeOldEvents would be waiting for the write lock held by rollover.
        boolean updated = false;
        while (!updated) {
            final SortedMap<Long, Path> existingPathMap = idToPathMap.get();
            final SortedMap<Long, Path> newPathMap = new TreeMap<>(new PathMapComparator());
            newPathMap.putAll(existingPathMap);

            final Iterator<Map.Entry<Long, Path>> itr = newPathMap.entrySet().iterator();
            while (itr.hasNext()) {
                final Map.Entry<Long, Path> entry = itr.next();
                final String filename = entry.getValue().toFile().getName();
                final String baseName = LuceneUtil.substringBefore(filename, ".");

                if (removed.contains(baseName)) {
                    itr.remove();
                }
            }

            updated = idToPathMap.compareAndSet(existingPathMap, newPathMap);
            logger.debug("After expiration, path map: {}", newPathMap);
        }

        purgeExpiredIndexes();
    }

    private void purgeExpiredIndexes() throws IOException {
        // Now that we have potentially removed expired Provenance Event Log Files, we can look at
        // whether or not we can delete any of the indexes. An index can be deleted if all of the
        // data that is associated with that index has already been deleted. In order to test this,
        // we will get the timestamp of the earliest event and then compare that to the latest timestamp
        // that would be indexed by the earliest index. If the event occurred after the timestamp of
        // the latest index, then we can just delete the entire index all together.

        // find all of the index directories
        final List<File> indexDirs = getAllIndexDirectories();
        if (indexDirs.size() < 2) {
            this.firstEventTimestamp = determineFirstEventTimestamp();
            return;
        }

        // Indexes are named "index-XXX" where the XXX is the timestamp of the earliest event that
        // could be in the index. Once we have finished with one index, we move on to another index,
        // but we don't move on until we are finished with the previous index.
        // Therefore, an efficient way to determine the latest timestamp of one index is to look at the
        // timestamp of the next index (these could potentially overlap for one millisecond). This is
        // efficient because we can determine the earliest timestamp of an index simply by looking at
        // the name of the Index's directory.
        final long latestTimestampOfFirstIndex = getIndexTimestamp(indexDirs.get(1));

        // Get the timestamp of the first event in the first Provenance Event Log File and the ID of the last event
        // in the event file.
        final List<File> logFiles = getSortedLogFiles();
        if (logFiles.isEmpty()) {
            this.firstEventTimestamp = System.currentTimeMillis();
            return;
        }

        final File firstLogFile = logFiles.get(0);
        long earliestEventTime = System.currentTimeMillis();
        long maxEventId = -1L;
        try (final RecordReader reader = RecordReaders.newRecordReader(firstLogFile, null, Integer.MAX_VALUE)) {
            final StandardProvenanceEventRecord event = reader.nextRecord();
            earliestEventTime = event.getEventTime();
            maxEventId = reader.getMaxEventId();
        } catch (final IOException ioe) {
            logger.warn("Unable to determine the maximum ID for Provenance Event Log File {}; values reported for the number of "
                    + "events in the Provenance Repository may be inaccurate.", firstLogFile);
        }

        // check if we can delete the index safely.
        if (latestTimestampOfFirstIndex <= earliestEventTime) {
            // we can safely delete the first index because the latest event in the index is an event
            // that has already been expired from the repository.
            final File indexingDirectory = indexDirs.get(0);
            getIndexManager().removeIndex(indexingDirectory);
            indexConfig.removeIndexDirectory(indexingDirectory);
            deleteDirectory(indexingDirectory);

            if (maxEventId > -1L) {
                indexConfig.setMinIdIndexed(maxEventId + 1L);
            }
        }

        this.firstEventTimestamp = earliestEventTime;
    }

    private long determineFirstEventTimestamp() {
        // Get the timestamp of the first event in the first Provenance Event Log File and the ID of the last event
        // in the event file.
        final List<File> logFiles = getSortedLogFiles();
        if (logFiles.isEmpty()) {
            return 0L;
        }

        for (final File logFile : logFiles) {
            try (final RecordReader reader = RecordReaders.newRecordReader(logFile, null, Integer.MAX_VALUE)) {
                final StandardProvenanceEventRecord event = reader.nextRecord();
                if (event != null) {
                    return event.getEventTime();
                }
            } catch (final IOException ioe) {
                logger.warn("Failed to obtain timestamp of first event from Provenance Event Log File {}", logFile);
            }
        }

        return 0L;
    }

    /**
     * Recursively deletes the given directory. If unable to delete the
     * directory, will emit a WARN level log event and move on.
     *
     * @param dir the directory to delete
     */
    private void deleteDirectory(final File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }

        final File[] children = dir.listFiles();
        if (children == null) {
            return;
        }

        for (final File child : children) {
            if (child.isDirectory()) {
                deleteDirectory(child);
            } else if (!child.delete()) {
                logger.warn("Unable to remove index directory {}; this directory should be cleaned up manually", child.getAbsolutePath());
            }
        }

        if (!dir.delete()) {
            logger.warn("Unable to remove index directory {}; this directory should be cleaned up manually", dir);
        }
    }

    /**
     * @return a List of all Index directories, sorted by timestamp of the
     * earliest event that could be present in the index
     */
    private List<File> getAllIndexDirectories() {
        final List<File> allIndexDirs = new ArrayList<>();
        for (final File storageDir : configuration.getStorageDirectories().values()) {
            final File[] indexDirs = storageDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(final File dir, final String name) {
                    return INDEX_PATTERN.matcher(name).matches();
                }
            });

            if (indexDirs != null) {
                for (final File indexDir : indexDirs) {
                    allIndexDirs.add(indexDir);
                }
            }
        }

        Collections.sort(allIndexDirs, new Comparator<File>() {
            @Override
            public int compare(final File o1, final File o2) {
                final long time1 = getIndexTimestamp(o1);
                final long time2 = getIndexTimestamp(o2);
                return Long.compare(time1, time2);
            }
        });

        return allIndexDirs;
    }

    /**
     * Takes a File that has a filename "index-" followed by a Long and returns
     * the value of that Long
     *
     * @param indexDirectory the index directory to obtain the timestamp for
     * @return the timestamp associated with the given index
     */
    private long getIndexTimestamp(final File indexDirectory) {
        final String name = indexDirectory.getName();
        final int dashIndex = name.indexOf("-");
        return Long.parseLong(name.substring(dashIndex + 1));
    }

    /**
     * Blocks the calling thread until the repository rolls over. This is
     * intended for unit testing.
     */
    public void waitForRollover() {
        final int count = rolloverCompletions.get();
        while (rolloverCompletions.get() == count) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
            }
        }
    }

    /**
     * @return the number of journal files that exist across all storage
     * directories
     */
    // made protected for testing purposes
    protected int getJournalCount() {
        // determine how many 'journals' we have in the journals directories
        int journalFileCount = 0;
        for (final File storageDir : configuration.getStorageDirectories().values()) {
            final File journalsDir = new File(storageDir, "journals");
            final File[] journalFiles = journalsDir.listFiles();
            if (journalFiles != null) {
                journalFileCount += journalFiles.length;
            }
        }

        return journalFileCount;
    }

    /**
     * Method is exposed for unit testing
     *
     * @param force whether or not to force a rollover.
     * @throws IOException if unable to complete rollover
     */
    void rolloverWithLock(final boolean force) throws IOException {
        writeLock.lock();
        try {
            rollover(force);
        } finally {
            writeLock.unlock();
        }
    }

    protected long getRolloverRetryMillis() {
        return 10000L;
    }

    /**
     * <p>
     * MUST be called with the write lock held.
     * </p>
     *
     * Rolls over the data in the journal files, merging them into a single
     * Provenance Event Log File, and compressing and indexing as needed.
     *
     * @param force if true, will force a rollover regardless of whether or not
     * data has been written
     * @throws IOException if unable to complete rollover
     */
    private void rollover(final boolean force) throws IOException {
        if (!configuration.isAllowRollover()) {
            return;
        }

        // If this is the first time we're creating the out stream, or if we
        // have written something to the stream, then roll over
        if (force || recordsWrittenSinceRollover.get() > 0L || dirtyWriterCount.get() > 0) {
            final List<File> journalsToMerge = new ArrayList<>();
            for (final RecordWriter writer : writers) {
                if (!writer.isClosed()) {
                    final File writerFile = writer.getFile();
                    journalsToMerge.add(writerFile);
                    try {
                        writer.close();
                    } catch (final IOException ioe) {
                        logger.warn("Failed to close {} due to {}", writer, ioe.toString());
                        if (logger.isDebugEnabled()) {
                            logger.warn("", ioe);
                        }
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                if (journalsToMerge.isEmpty()) {
                    logger.debug("No journals to merge; all RecordWriters were already closed");
                } else {
                    logger.debug("Going to merge {} files for journals starting with ID {}", journalsToMerge.size(), LuceneUtil.substringBefore(journalsToMerge.get(0).getName(), "."));
                }
            }

            // Choose a storage directory to store the merged file in.
            final long storageDirIdx = storageDirectoryIndex.getAndIncrement();
            final List<File> storageDirs = new ArrayList<>(configuration.getStorageDirectories().values());
            final File storageDir = storageDirs.get((int) (storageDirIdx % storageDirs.size()));

            Future<?> future = null;
            if (!journalsToMerge.isEmpty()) {
                // Run the rollover logic in a background thread.
                final AtomicReference<Future<?>> futureReference = new AtomicReference<>();
                final AtomicInteger retryAttempts = new AtomicInteger(MAX_JOURNAL_ROLLOVER_RETRIES);
                final int recordsWritten = recordsWrittenSinceRollover.getAndSet(0);
                final Runnable rolloverRunnable = new Runnable() {
                    @Override
                    public void run() {
                        File fileRolledOver = null;

                        try {
                            try {
                                fileRolledOver = mergeJournals(journalsToMerge, getMergeFile(journalsToMerge, storageDir), eventReporter);
                            } catch (final IOException ioe) {
                                logger.error("Failed to merge Journal Files {} into a Provenance Log File due to {}", journalsToMerge, ioe.toString());
                                logger.error("", ioe);
                            }

                            if (fileRolledOver != null) {

                                final File file = fileRolledOver;

                                // update our map of id to Path
                                // We need to make sure that another thread doesn't also update the map at the same time. We cannot
                                // use the write lock when purging old events, and we want to use the same approach here.
                                boolean updated = false;
                                final Long fileFirstEventId = Long.valueOf(LuceneUtil.substringBefore(fileRolledOver.getName(), "."));
                                while (!updated) {
                                    final SortedMap<Long, Path> existingPathMap = idToPathMap.get();
                                    final SortedMap<Long, Path> newIdToPathMap = new TreeMap<>(new PathMapComparator());
                                    newIdToPathMap.putAll(existingPathMap);
                                    newIdToPathMap.put(fileFirstEventId, file.toPath());
                                    updated = idToPathMap.compareAndSet(existingPathMap, newIdToPathMap);
                                }

                                final TimedCountSize countSize = updateCounts.getAggregateValue(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES));
                                logger.info("Successfully Rolled over Provenance Event file containing {} records. In the past 5 minutes, "
                                    + "{} events have been written to the Provenance Repository, totaling {}",
                                    recordsWritten, countSize.getCount(), FormatUtils.formatDataSize(countSize.getSize()));
                            }

                            //if files were rolled over or if out of retries stop the future
                            if (fileRolledOver != null || retryAttempts.decrementAndGet() == 0) {

                                if (fileRolledOver == null && retryAttempts.get() == 0) {
                                    logger.error("Failed to merge Journal Files {} after {} attempts.", journalsToMerge, MAX_JOURNAL_ROLLOVER_RETRIES);
                                }

                                rolloverCompletions.getAndIncrement();

                                // Cancel the future so that we don't run anymore
                                Future<?> future;
                                while ((future = futureReference.get()) == null) {
                                    try {
                                        Thread.sleep(10L);
                                    } catch (final InterruptedException ie) {
                                    }
                                }
                                future.cancel(false);

                            } else {
                                logger.warn("Couldn't merge journals. Will try again. journalsToMerge: {}, storageDir: {}", journalsToMerge, storageDir);
                            }
                        } catch (final Exception e) {
                            logger.error("Failed to merge journals. Will try again. journalsToMerge: {}, storageDir: {}, cause: {}", journalsToMerge, storageDir, e.toString());
                            logger.error("", e);
                        }
                    }
                };

                // We are going to schedule the future to run immediately and then repeat every 10 seconds. This allows us to keep retrying if we
                // fail for some reason. When we succeed or if retries are exceeded, the Runnable will cancel itself.
                future = rolloverExecutor.scheduleWithFixedDelay(rolloverRunnable, 0, getRolloverRetryMillis(), TimeUnit.MILLISECONDS);
                futureReference.set(future);
            }

            streamStartTime.set(System.currentTimeMillis());
            bytesWrittenSinceRollover.set(0);

            // We don't want to create new 'writers' until the number of unmerged journals falls below our threshold. So we wait
            // here before we repopulate the 'writers' member variable and release the lock.
            int journalFileCount = getJournalCount();
            long repoSize = getSize(getLogFiles(), 0L);
            final int journalCountThreshold = configuration.getJournalCount() * 5;
            final long sizeThreshold = (long) (configuration.getMaxStorageCapacity() * ROLLOVER_HIGH_WATER);

            // check if we need to apply backpressure.
            // If we have too many journal files, or if the repo becomes too large, backpressure is necessary. Without it,
            // if the rate at which provenance events are registered exceeds the rate at which we can compress/merge/index them,
            // then eventually we will end up with all of the data stored in the 'journals' directory and not yet indexed. This
            // would mean that the data would never even be accessible. In order to prevent this, if we exceeds 110% of the configured
            // max capacity for the repo, or if we have 5 sets of journal files waiting to be merged, we will block here until
            // that is no longer the case.
            if (journalFileCount > journalCountThreshold || repoSize > sizeThreshold) {
                final long stopTheWorldStart = System.nanoTime();

                logger.warn("The rate of the dataflow is exceeding the provenance recording rate. "
                        + "Slowing down flow to accommodate. Currently, there are {} journal files ({} bytes) and "
                        + "threshold for blocking is {} ({} bytes)", journalFileCount, repoSize, journalCountThreshold, sizeThreshold);
                eventReporter.reportEvent(Severity.WARNING, "Provenance Repository", "The rate of the dataflow is "
                        + "exceeding the provenance recording rate. Slowing down flow to accommodate");

                while (journalFileCount > journalCountThreshold || repoSize > sizeThreshold) {
                    // if a shutdown happens while we are in this loop, kill the rollover thread and break
                    if (this.closed.get()) {
                        if (future != null) {
                            future.cancel(true);
                        }

                        break;
                    }

                    if (repoSize > sizeThreshold) {
                        logger.debug("Provenance Repository has exceeded its size threshold; will trigger purging of oldest events");
                        purgeOldEvents();

                        journalFileCount = getJournalCount();
                        repoSize = getSize(getLogFiles(), 0L);
                        continue;
                    } else {
                        // if we are constrained by the number of journal files rather than the size of the repo,
                        // then we will just sleep a bit because another thread is already actively merging the journals,
                        // due to the runnable that we scheduled above
                        try {
                            Thread.sleep(100L);
                        } catch (final InterruptedException ie) {
                        }
                    }

                    logger.debug("Provenance Repository is still behind. Keeping flow slowed down "
                            + "to accommodate. Currently, there are {} journal files ({} bytes) and "
                            + "threshold for blocking is {} ({} bytes)", journalFileCount, repoSize, journalCountThreshold, sizeThreshold);

                    journalFileCount = getJournalCount();
                    repoSize = getSize(getLogFiles(), 0L);
                }

                final long stopTheWorldNanos = System.nanoTime() - stopTheWorldStart;
                backpressurePauseMillis.add(new TimestampedLong(stopTheWorldNanos));
                final TimestampedLong pauseNanosLastFiveMinutes = backpressurePauseMillis.getAggregateValue(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES));
                logger.info("Provenance Repository has now caught up with rolling over journal files. Current number of "
                    + "journal files to be rolled over is {}. Provenance Repository Back Pressure paused Session commits for {} ({} total in the last 5 minutes).",
                    journalFileCount, FormatUtils.formatNanos(stopTheWorldNanos, true), FormatUtils.formatNanos(pauseNanosLastFiveMinutes.getValue(), true));
            }

            // we've finished rolling over successfully. Create new writers and reset state.
            writers = createWriters(configuration, idGenerator.get());
            dirtyWriterCount.set(0);
            streamStartTime.set(System.currentTimeMillis());
            recordsWrittenSinceRollover.getAndSet(0);
        }
    }

    // protected for use in unit tests
    protected Set<File> recoverJournalFiles() throws IOException {
        if (!configuration.isAllowRollover()) {
            return Collections.emptySet();
        }

        final Map<String, List<File>> journalMap = new HashMap<>();

        // Map journals' basenames to the files with that basename.
        final List<File> storageDirs = new ArrayList<>(configuration.getStorageDirectories().values());
        for (final File storageDir : storageDirs) {
            final File journalDir = new File(storageDir, "journals");
            if (!journalDir.exists()) {
                continue;
            }

            final File[] journalFiles = journalDir.listFiles();
            if (journalFiles == null) {
                continue;
            }

            for (final File journalFile : journalFiles) {
                if (journalFile.isDirectory()) {
                    continue;
                }

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
            final File mergedFile = mergeJournals(journalFileSet, getMergeFile(journalFileSet, storageDir), eventReporter);
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

    protected List<File> filterUnavailableFiles(final List<File> journalFiles) {
        return journalFiles.stream().filter(file -> file.exists()).collect(Collectors.toList());
    }

    /**
     * <p>
     * Merges all of the given Journal Files into a single, merged Provenance
     * Event Log File. As these records are merged, they will be compressed, if
     * the repository is configured to compress records, and will be indexed.
     * </p>
     *
     * <p>
     * If the repository is configured to compress the data, the file written to
     * may not be the same as the <code>suggestedMergeFile</code>, as a filename
     * extension of '.gz' may be appended. If the journals are successfully
     * merged, the file that they were merged into will be returned. If unable
     * to merge the records (for instance, because the repository has been
     * closed or because the list of journal files was empty), this method will
     * return <code>null</code>.
     * </p>
     *
     * @param journalFiles the journal files to merge
     * @param suggestedMergeFile the file to write the merged records to
     * @param eventReporter the event reporter to report any warnings or errors
     * to; may be null.
     *
     * @return the file that the given journals were merged into, or
     * <code>null</code> if no records were merged.
     *
     * @throws IOException if a problem occurs writing to the mergedFile,
     * reading from a journal, or updating the Lucene Index.
     */
    File mergeJournals(final List<File> journalFiles, final File suggestedMergeFile, final EventReporter eventReporter) throws IOException {
        logger.debug("Merging {} to {}", journalFiles, suggestedMergeFile);
        if (this.closed.get()) {
            logger.info("Provenance Repository has been closed; will not merge journal files to {}", suggestedMergeFile);
            return null;
        }

        if (journalFiles.isEmpty()) {
            logger.debug("Couldn't merge journals: Journal Files is empty; won't merge journals");
            return null;
        }

        Collections.sort(journalFiles, new Comparator<File>() {
            @Override
            public int compare(final File o1, final File o2) {
                final String suffix1 = LuceneUtil.substringAfterLast(o1.getName(), ".");
                final String suffix2 = LuceneUtil.substringAfterLast(o2.getName(), ".");

                try {
                    final int journalIndex1 = Integer.parseInt(suffix1);
                    final int journalIndex2 = Integer.parseInt(suffix2);
                    return Integer.compare(journalIndex1, journalIndex2);
                } catch (final NumberFormatException nfe) {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        });

        // Search for any missing files. At this point they should have been written to disk otherwise cannot continue.
        // Missing files is most likely due to incomplete cleanup of files post merge
        final List<File> availableFiles = filterUnavailableFiles(journalFiles);
        final int numAvailableFiles = availableFiles.size();

        // check if we have all of the "partial" files for the journal.
        if (numAvailableFiles > 0) {
            if (suggestedMergeFile.exists()) {
                // we have all "partial" files and there is already a merged file. Delete the data from the index
                // because the merge file may not be fully merged. We will re-merge.
                logger.warn("Merged Journal File {} already exists; however, all partial journal files also exist "
                        + "so assuming that the merge did not finish. Repeating procedure in order to ensure consistency.");

                final DeleteIndexAction deleteAction = new DeleteIndexAction(this, indexConfig, getIndexManager());
                try {
                    deleteAction.execute(suggestedMergeFile);
                } catch (final Exception e) {
                    logger.warn("Failed to delete records from Journal File {} from the index; this could potentially result in duplicates. Failure was due to {}", suggestedMergeFile, e.toString());
                    if (logger.isDebugEnabled()) {
                        logger.warn("", e);
                    }
                }

                // Since we only store the file's basename, block offset, and event ID, and because the newly created file could end up on
                // a different Storage Directory than the original, we need to ensure that we delete both the partially merged
                // file and the TOC file. Otherwise, we could get the wrong copy and have issues retrieving events.
                if (!suggestedMergeFile.delete()) {
                    logger.error("Failed to delete partially written Provenance Journal File {}. This may result in events from this journal "
                            + "file not being able to be displayed. This file should be deleted manually.", suggestedMergeFile);
                }

                final File tocFile = TocUtil.getTocFile(suggestedMergeFile);
                if (tocFile.exists() && !tocFile.delete()) {
                    logger.error("Failed to delete .toc file {}; this may result in not being able to read the Provenance Events from the {} Journal File. "
                            + "This can be corrected by manually deleting the {} file", tocFile, suggestedMergeFile, tocFile);
                }
            }
        } else {
            logger.warn("Cannot merge journal files {} because they do not exist on disk", journalFiles);
            return null;
        }

        final long startNanos = System.nanoTime();

        // Map each journal to a RecordReader
        final List<RecordReader> readers = new ArrayList<>();
        int records = 0;

        final boolean isCompress = configuration.isCompressOnRollover();
        final File writerFile = isCompress ? new File(suggestedMergeFile.getParentFile(), suggestedMergeFile.getName() + ".gz") : suggestedMergeFile;

        try {
            for (final File journalFile : availableFiles) {
                try {
                    // Use MAX_VALUE for number of chars because we don't want to truncate the value as we write it
                    // out. This allows us to later decide that we want more characters and still be able to retrieve
                    // the entire event.
                    readers.add(RecordReaders.newRecordReader(journalFile, null, Integer.MAX_VALUE));
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

            long minEventId = 0L;
            long earliestTimestamp = System.currentTimeMillis();
            for (final RecordReader reader : readers) {
                StandardProvenanceEventRecord record = null;

                try {
                    record = reader.nextRecord();
                } catch (final EOFException eof) {
                    // record will be null and reader can no longer be used
                } catch (final Exception e) {
                    logger.warn("Failed to generate Provenance Event Record from Journal due to " + e + "; it's "
                            + "possible that the record wasn't completely written to the file. This journal will be "
                            + "skipped.");
                    if (logger.isDebugEnabled()) {
                        logger.warn("", e);
                    }

                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to read Provenance Event "
                                + "Record from Journal due to " + e + "; it's possible that the record wasn't "
                                + "completely written to the file. This journal will be skipped.");
                    }
                }

                if (record == null) {
                    continue;
                }

                if (record.getEventTime() < earliestTimestamp) {
                    earliestTimestamp = record.getEventTime();
                }

                if (record.getEventId() < minEventId) {
                    minEventId = record.getEventId();
                }

                recordToReaderMap.put(record, reader);
            }

            // We want to keep track of the last 1000 events in the files so that we can add them to 'ringBuffer'.
            // However, we don't want to add them directly to ringBuffer, because once they are added to ringBuffer, they are
            // available in query results. As a result, we can have the issue where we've not finished indexing the file
            // but we try to create the lineage for events in that file. In order to avoid this, we will add the records
            // to a temporary RingBuffer and after we finish merging the records will then copy the data to the
            // ringBuffer provided as a method argument.
            final RingBuffer<ProvenanceEventRecord> latestRecords = new RingBuffer<>(1000);

            // loop over each entry in the map, persisting the records to the merged file in order, and populating the map
            // with the next entry from the journal file from which the previous record was written.
            try (final RecordWriter writer = RecordWriters.newSchemaRecordWriter(writerFile, idGenerator, configuration.isCompressOnRollover(), true)) {
                writer.writeHeader(minEventId);

                final IndexingAction indexingAction = createIndexingAction();

                final File indexingDirectory = indexConfig.getWritableIndexDirectory(writerFile, earliestTimestamp);
                long maxId = 0L;

                final BlockingQueue<Tuple<StandardProvenanceEventRecord, Integer>> eventQueue = new LinkedBlockingQueue<>(100);
                final AtomicBoolean finishedAdding = new AtomicBoolean(false);
                final List<Future<?>> futures = new ArrayList<>();

                final EventIndexWriter indexWriter = getIndexManager().borrowIndexWriter(indexingDirectory);
                try {
                    final ExecutorService exec = Executors.newFixedThreadPool(configuration.getIndexThreadPoolSize(), new ThreadFactory() {
                        @Override
                        public Thread newThread(final Runnable r) {
                            final Thread t = Executors.defaultThreadFactory().newThread(r);
                            t.setName("Index Provenance Events");
                            return t;
                        }
                    });

                    final AtomicInteger indexingFailureCount = new AtomicInteger(0);
                    try {
                        for (int i = 0; i < configuration.getIndexThreadPoolSize(); i++) {
                            final Callable<Object> callable = new Callable<Object>() {
                                @Override
                                public Object call() throws IOException {
                                    while (!eventQueue.isEmpty() || !finishedAdding.get()) {
                                        try {
                                            final Tuple<StandardProvenanceEventRecord, Integer> tuple;
                                            try {
                                                tuple = eventQueue.poll(10, TimeUnit.MILLISECONDS);
                                            } catch (final InterruptedException ie) {
                                                Thread.currentThread().interrupt();
                                                continue;
                                            }

                                            if (tuple == null) {
                                                continue;
                                            }

                                            indexingAction.index(tuple.getKey(), indexWriter.getIndexWriter(), tuple.getValue());
                                        } catch (final Throwable t) {
                                            logger.error("Failed to index Provenance Event for " + writerFile + " to " + indexingDirectory, t);
                                            if (indexingFailureCount.incrementAndGet() >= MAX_INDEXING_FAILURE_COUNT) {
                                                return null;
                                            }
                                        }
                                    }

                                    return null;
                                }
                            };

                            final Future<?> future = exec.submit(callable);
                            futures.add(future);
                        }

                        boolean indexEvents = true;
                        while (!recordToReaderMap.isEmpty()) {
                            final StandardProvenanceEventRecord record = recordToReaderMap.firstKey();
                            final RecordReader reader = recordToReaderMap.get(record);

                            writer.writeRecord(record);
                            final int blockIndex = writer.getTocWriter().getCurrentBlockIndex();

                            boolean accepted = false;
                            while (!accepted && indexEvents) {
                                try {
                                    accepted = eventQueue.offer(new Tuple<>(record, blockIndex), 10, TimeUnit.MILLISECONDS);
                                } catch (final InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                }

                                // If we weren't able to add anything to the queue, check if we have reached our max failure count.
                                // We do this here because if we do reach our max failure count, all of the indexing threads will stop
                                // performing their jobs. As a result, the queue will fill and we won't be able to add anything to it.
                                // So, if the queue is filled, we will check if this is the case.
                                if (!accepted && indexingFailureCount.get() >= MAX_INDEXING_FAILURE_COUNT) {
                                    indexEvents = false;  // don't add anything else to the queue.
                                    eventQueue.clear();

                                    final String warning = String.format("Indexing Provenance Events for %s has failed %s times. This exceeds the maximum threshold of %s failures, "
                                            + "so no more Provenance Events will be indexed for this Provenance file.", writerFile, indexingFailureCount.get(), MAX_INDEXING_FAILURE_COUNT);
                                    logger.warn(warning);
                                    if (eventReporter != null) {
                                        eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, warning);
                                    }
                                }
                            }

                            maxId = record.getEventId();

                            latestRecords.add(truncateAttributes(record));
                            records++;

                            // Remove this entry from the map
                            recordToReaderMap.remove(record);

                            // Get the next entry from this reader and add it to the map
                            StandardProvenanceEventRecord nextRecord = null;

                            try {
                                nextRecord = reader.nextRecord();
                            } catch (final EOFException eof) {
                                // record will be null and reader can no longer be used
                            } catch (final Exception e) {
                                logger.warn("Failed to generate Provenance Event Record from Journal due to " + e
                                        + "; it's possible that the record wasn't completely written to the file. "
                                        + "The remainder of this journal will be skipped.");
                                if (logger.isDebugEnabled()) {
                                    logger.warn("", e);
                                }

                                if (eventReporter != null) {
                                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to read "
                                            + "Provenance Event Record from Journal due to " + e + "; it's possible "
                                            + "that the record wasn't completely written to the file. The remainder "
                                            + "of this journal will be skipped.");
                                }
                            }

                            if (nextRecord != null) {
                                recordToReaderMap.put(nextRecord, reader);
                            }
                        }
                    } finally {
                        finishedAdding.set(true);
                        exec.shutdown();
                    }

                    for (final Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (final ExecutionException ee) {
                            final Throwable t = ee.getCause();
                            if (t instanceof RuntimeException) {
                                throw (RuntimeException) t;
                            }

                            throw new RuntimeException(t);
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Thread interrupted");
                        }
                    }
                } finally {
                    getIndexManager().returnIndexWriter(indexWriter);
                }

                indexConfig.setMaxIdIndexed(maxId);
            }

            // record should now be available in the repository. We can copy the values from latestRecords to ringBuffer.
            final RingBuffer<ProvenanceEventRecord> latestRecordBuffer = this.latestRecords;
            latestRecords.forEach(new ForEachEvaluator<ProvenanceEventRecord>() {
                @Override
                public boolean evaluate(final ProvenanceEventRecord event) {
                    latestRecordBuffer.add(event);
                    return true;
                }
            });
        } finally {
            for (final RecordReader reader : readers) {
                try {
                    reader.close();
                } catch (final IOException ioe) {
                }
            }
        }

        // Success. Remove all of the journal files, as they're no longer needed, now that they've been merged.
        for (final File journalFile : availableFiles) {
            if (!journalFile.delete() && journalFile.exists()) {
                logger.warn("Failed to remove temporary journal file {}; this file should be cleaned up manually", journalFile.getAbsolutePath());

                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to remove temporary journal file "
                            + journalFile.getAbsolutePath() + "; this file should be cleaned up manually");
                }
            }

            final File tocFile = TocUtil.getTocFile(journalFile);
            if (!tocFile.delete() && tocFile.exists()) {
                logger.warn("Failed to remove temporary journal TOC file {}; this file should be cleaned up manually", tocFile.getAbsolutePath());

                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to remove temporary journal TOC file "
                            + tocFile.getAbsolutePath() + "; this file should be cleaned up manually");
                }
            }
        }

        if (records == 0) {
            writerFile.delete();
            logger.debug("Couldn't merge journals: No Records to merge");
            return null;
        } else {
            final long nanos = System.nanoTime() - startNanos;
            final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
            logger.info("Successfully merged {} journal files ({} records) into single Provenance Log File {} in {} milliseconds", numAvailableFiles, records, suggestedMergeFile, millis);
        }

        return writerFile;
    }

    /**
     * This method is protected and exists for testing purposes. This allows
     * unit tests to extend this class and override the createIndexingAction so
     * that they can mock out the Indexing Action to throw Exceptions, count
     * events indexed, etc.
     */
    protected IndexingAction createIndexingAction() {
        return new IndexingAction(configuration.getSearchableFields(), configuration.getSearchableAttributes());
    }

    private StandardProvenanceEventRecord truncateAttributes(final StandardProvenanceEventRecord original) {
        boolean requireTruncation = false;

        for (final String updatedAttr : original.getUpdatedAttributes().values()) {
            if (updatedAttr != null && updatedAttr.length() > maxAttributeChars) {
                requireTruncation = true;
                break;
            }
        }

        if (!requireTruncation) {
            for (final String previousAttr : original.getPreviousAttributes().values()) {
                if (previousAttr != null && previousAttr.length() > maxAttributeChars) {
                    requireTruncation = true;
                    break;
                }
            }
        }

        if (!requireTruncation) {
            return original;
        }

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder().fromEvent(original);
        builder.setAttributes(truncateAttributes(original.getPreviousAttributes()), truncateAttributes(original.getUpdatedAttributes()));
        final StandardProvenanceEventRecord truncated = builder.build();
        truncated.setEventId(original.getEventId());
        return truncated;
    }

    private Map<String, String> truncateAttributes(final Map<String, String> original) {
        final Map<String, String> truncatedAttrs = new HashMap<>();
        for (final Map.Entry<String, String> entry : original.entrySet()) {
            String value = entry.getValue() != null && entry.getValue().length() > this.maxAttributeChars
                    ? entry.getValue().substring(0, this.maxAttributeChars) : entry.getValue();
            truncatedAttrs.put(entry.getKey(), value);
        }
        return truncatedAttrs;
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return new ArrayList<>(configuration.getSearchableFields());
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return new ArrayList<>(configuration.getSearchableAttributes());
    }

    QueryResult queryEvents(final Query query, final NiFiUser user) throws IOException {
        final QuerySubmission submission = submitQuery(query, user);
        final QueryResult result = submission.getResult();
        while (!result.isFinished()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        if (result.getError() != null) {
            throw new IOException(result.getError());
        }
        logger.info("{} got {} hits", query, result.getTotalHitCount());
        return result;
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser user) {
        final String userId = user == null ? null : user.getIdentity();
        final int numQueries = querySubmissionMap.size();

        if (numQueries > MAX_UNDELETED_QUERY_RESULTS) {
            throw new IllegalStateException("Cannot process query because there are currently " + numQueries + " queries whose results have not "
                    + "been deleted due to poorly behaving clients not issuing DELETE requests. Please try again later.");
        }

        if (query.getEndDate() != null && query.getStartDate() != null && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }

        if (query.getSearchTerms().isEmpty() && query.getStartDate() == null && query.getEndDate() == null) {
            final AsyncQuerySubmission result = new AsyncQuerySubmission(query, 1, userId);

            if (latestRecords.getSize() >= query.getMaxResults()) {
                final List<ProvenanceEventRecord> latestList = filterUnauthorizedEvents(latestRecords.asList(), user);
                final List<ProvenanceEventRecord> trimmed;
                if (latestList.size() > query.getMaxResults()) {
                    trimmed = latestList.subList(latestList.size() - query.getMaxResults(), latestList.size());
                } else {
                    trimmed = latestList;
                }

                Long maxEventId = getMaxEventId();
                if (maxEventId == null) {
                    result.getResult().update(Collections.<ProvenanceEventRecord>emptyList(), 0L);
                    maxEventId = 0L;
                }
                Long minIndexedId = indexConfig.getMinIdIndexed();
                if (minIndexedId == null) {
                    minIndexedId = 0L;
                }

                final long totalNumDocs = maxEventId - minIndexedId;

                result.getResult().update(trimmed, totalNumDocs);
            } else {
                queryExecService.submit(new GetMostRecentRunnable(query, result, user));
            }

            querySubmissionMap.put(query.getIdentifier(), result);
            return result;
        }

        final AtomicInteger retrievalCount = new AtomicInteger(0);
        final List<File> indexDirectories = indexConfig.getIndexDirectories(
                query.getStartDate() == null ? null : query.getStartDate().getTime(),
                query.getEndDate() == null ? null : query.getEndDate().getTime());
        final AsyncQuerySubmission result = new AsyncQuerySubmission(query, indexDirectories.size(), userId);
        querySubmissionMap.put(query.getIdentifier(), result);

        if (indexDirectories.isEmpty()) {
            result.getResult().update(Collections.<ProvenanceEventRecord>emptyList(), 0L);
        } else {
            for (final File indexDir : indexDirectories) {
                result.addQueryExecution(queryExecService.submit(new QueryRunnable(query, result, user, indexDir, retrievalCount)));
            }
        }

        return result;
    }

    /**
     * This is for testing only and not actually used other than in debugging
     *
     * @param luceneQuery the lucene query to execute
     * @return an Iterator of ProvenanceEventRecord that match the query
     * @throws IOException if unable to perform the query
     */
    Iterator<ProvenanceEventRecord> queryLucene(final org.apache.lucene.search.Query luceneQuery) throws IOException {
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

                        final TopDocs topDocs = searcher.search(luceneQuery, 10000000);
                        logger.info("For {}, Top Docs has {} hits; reading Lucene results", indexDirectory, topDocs.scoreDocs.length);

                        if (topDocs.totalHits > 0) {
                            for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {
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
                                reader = RecordReaders.newRecordReader(file, allLogFiles, maxAttributeChars);
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

    Lineage computeLineage(final String flowFileUuid, final NiFiUser user) throws IOException {
        return computeLineage(Collections.<String>singleton(flowFileUuid), user, LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }

    private Lineage computeLineage(final Collection<String> flowFileUuids, final NiFiUser user, final LineageComputationType computationType, final Long eventId, final Long startTimestamp,
            final Long endTimestamp) throws IOException {
        final AsyncLineageSubmission submission = submitLineageComputation(flowFileUuids, user, computationType, eventId, startTimestamp, endTimestamp);
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
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        final ProvenanceEventRecord event;
        try {
            event = getEvent(eventId);
        } catch (final Exception e) {
            logger.error("Failed to retrieve Provenance Event with ID " + eventId + " to calculate data lineage due to: " + e, e);
            final AsyncLineageSubmission result = new AsyncLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId,
                Collections.<String> emptySet(), 1, user == null ? null : user.getIdentity());
            result.getResult().setError("Failed to retrieve Provenance Event with ID " + eventId + ". See logs for more information.");
            return result;
        }

        if (event == null) {
            final AsyncLineageSubmission result = new AsyncLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId,
                Collections.<String> emptySet(), 1, user == null ? null : user.getIdentity());
            result.getResult().setError("Could not find Provenance Event with ID " + eventId);
            lineageSubmissionMap.put(result.getLineageIdentifier(), result);
            return result;
        }

        return submitLineageComputation(Collections.singleton(event.getFlowFileUuid()), user, LineageComputationType.FLOWFILE_LINEAGE, eventId, event.getLineageStartDate(), Long.MAX_VALUE);
    }

    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user) {
        return submitLineageComputation(Collections.singleton(flowFileUuid), user, LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }

    private AsyncLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids, final NiFiUser user, final LineageComputationType computationType,
            final Long eventId, final long startTimestamp, final long endTimestamp) {
        final List<File> indexDirs = indexConfig.getIndexDirectories(startTimestamp, endTimestamp);
        final AsyncLineageSubmission result = new AsyncLineageSubmission(computationType, eventId, flowFileUuids, indexDirs.size(), user == null ? null : user.getIdentity());
        lineageSubmissionMap.put(result.getLineageIdentifier(), result);

        for (final File indexDir : indexDirs) {
            queryExecService.submit(new ComputeLineageRunnable(flowFileUuids, user, result, indexDir));
        }

        return result;
    }

    @Override
    public AsyncLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        final String userId = user == null ? null : user.getIdentity();

        try {
            final ProvenanceEventRecord event = getEvent(eventId);
            if (event == null) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.emptyList(), 0L);
                return submission;
            }

            switch (event.getEventType()) {
                case CLONE:
                case FORK:
                case JOIN:
                case REPLAY:
                    return submitLineageComputation(event.getChildUuids(), user, LineageComputationType.EXPAND_CHILDREN, eventId, event.getEventTime(), Long.MAX_VALUE);
                default:
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1, userId);
                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its children cannot be expanded");
                    return submission;
            }
        } catch (final IOException ioe) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.<String>emptyList(), 1, userId);
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
    public AsyncLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        final String userId = user == null ? null : user.getIdentity();

        try {
            final ProvenanceEventRecord event = getEvent(eventId);
            if (event == null) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.emptyList(), 0L);
                return submission;
            }

            switch (event.getEventType()) {
                case JOIN:
                case FORK:
                case CLONE:
                case REPLAY:
                    return submitLineageComputation(event.getParentUuids(), user, LineageComputationType.EXPAND_PARENTS, eventId, event.getLineageStartDate(), event.getEventTime());
                default: {
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.<String>emptyList(), 1, userId);
                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its parents cannot be expanded");
                    return submission;
                }
            }
        } catch (final IOException ioe) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.<String>emptyList(), 1, userId);
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
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, final NiFiUser user) {
        final AsyncLineageSubmission submission = lineageSubmissionMap.get(lineageIdentifier);
        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because no user id was provided in the lineage request.");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because " + user.getIdentity() + " is not the user who submitted the request.");
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        final QuerySubmission submission = querySubmissionMap.get(queryIdentifier);

        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because no user id was provided in the provenance request.");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because " + user.getIdentity() + " is not the user who submitted the request.");
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

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) throws IOException {
        final ProvenanceEventRecord event = getEvent(id);
        if (event == null) {
            return null;
        }

        authorize(event, user);
        return event;
    }

    private boolean needToRollover() {
        final long writtenSinceRollover = bytesWrittenSinceRollover.get();

        if (writtenSinceRollover >= maxPartitionBytes) {
            return true;
        }

        if ((dirtyWriterCount.get() > 0) || (writtenSinceRollover > 0 && System.currentTimeMillis() > streamStartTime.get() + maxPartitionMillis)) {
            return true;
        }

        return false;
    }

    /**
     * @return a List of all Provenance Event Log Files, sorted in ascending
     * order by the first Event ID in each file
     */
    private List<File> getSortedLogFiles() {
        final List<Path> paths = new ArrayList<>(getAllLogFiles());
        Collections.sort(paths, new Comparator<Path>() {
            @Override
            public int compare(final Path o1, final Path o2) {
                return Long.compare(getFirstEventId(o1.toFile()), getFirstEventId(o2.toFile()));
            }
        });

        final List<File> files = new ArrayList<>(paths.size());
        for (final Path path : paths) {
            files.add(path.toFile());
        }
        return files;
    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    /**
     * Returns the Event ID of the first event in the given Provenance Event Log
     * File.
     *
     * @param logFile the log file from which to obtain the first Event ID
     * @return the ID of the first event in the given log file
     */
    private long getFirstEventId(final File logFile) {
        final String name = logFile.getName();
        final int dotIndex = name.indexOf(".");
        return Long.parseLong(name.substring(0, dotIndex));
    }

    public Collection<Path> getAllLogFiles() {
        final SortedMap<Long, Path> map = idToPathMap.get();
        return map == null ? new ArrayList<>() : map.values();
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
        private final NiFiUser user;

        public GetMostRecentRunnable(final Query query, final AsyncQuerySubmission submission, final NiFiUser user) {
            this.query = query;
            this.submission = submission;
            this.user = user;
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

                final List<ProvenanceEventRecord> mostRecent = getEvents(startIndex, maxResults, user);
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
        private final NiFiUser user;
        private final File indexDir;
        private final AtomicInteger retrievalCount;

        public QueryRunnable(final Query query, final AsyncQuerySubmission submission, final NiFiUser user, final File indexDir, final AtomicInteger retrievalCount) {
            this.query = query;
            this.submission = submission;
            this.user = user;
            this.indexDir = indexDir;
            this.retrievalCount = retrievalCount;
        }

        @Override
        public void run() {
            try {
                final IndexSearch search = new IndexSearch(PersistentProvenanceRepository.this, indexDir, getIndexManager(), maxAttributeChars);
                final StandardQueryResult queryResult = search.search(query, user, retrievalCount, firstEventTimestamp);
                submission.getResult().update(queryResult.getMatchingEvents(), queryResult.getTotalHitCount());
            } catch (final Throwable t) {
                logger.error("Failed to query Provenance Repository Index {} due to {}", indexDir, t.toString());
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
        private final NiFiUser user;
        private final File indexDir;
        private final AsyncLineageSubmission submission;

        public ComputeLineageRunnable(final Collection<String> flowFileUuids, final NiFiUser user, final AsyncLineageSubmission submission, final File indexDir) {
            this.flowFileUuids = flowFileUuids;
            this.user = user;
            this.submission = submission;
            this.indexDir = indexDir;
        }

        @Override
        public void run() {
            if (submission.isCanceled()) {
                return;
            }

            try {
                final DocumentToEventConverter converter = new DocumentToEventConverter() {
                    @Override
                    public Set<ProvenanceEventRecord> convert(TopDocs topDocs, IndexReader indexReader) throws IOException {
                        // Always authorized. We do this because we need to pull back the event, regardless of whether or not
                        // the user is truly authorized, because instead of ignoring unauthorized events, we want to replace them.
                        final EventAuthorizer authorizer = EventAuthorizer.GRANT_ALL;
                        final DocsReader docsReader = new DocsReader();
                        return docsReader.read(topDocs, authorizer, indexReader, getAllLogFiles(), new AtomicInteger(0), Integer.MAX_VALUE, maxAttributeChars);
                    }
                };

                final Set<ProvenanceEventRecord> matchingRecords = LineageQuery.computeLineageForFlowFiles(getIndexManager(), indexDir, null, flowFileUuids, converter);

                final StandardLineageResult result = submission.getResult();
                result.update(replaceUnauthorizedWithPlaceholders(matchingRecords, user), matchingRecords.size());

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
                    if (entry.getValue().isCanceled() || result.isFinished() && result.getExpiration().before(now)) {
                        queryIterator.remove();
                    }
                }

                final Iterator<Map.Entry<String, AsyncLineageSubmission>> lineageIterator = lineageSubmissionMap.entrySet().iterator();
                while (lineageIterator.hasNext()) {
                    final Map.Entry<String, AsyncLineageSubmission> entry = lineageIterator.next();

                    final StandardLineageResult result = entry.getValue().getResult();
                    if (entry.getValue().isCanceled() || result.isFinished() && result.getExpiration().before(now)) {
                        lineageIterator.remove();
                    }
                }
            } catch (final Throwable t) {
                logger.error("Failed to expire Provenance Query Results due to {}", t.toString());
                logger.error("", t);
            }
        }
    }
}
