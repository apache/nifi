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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.expiration.ExpirationAction;
import org.apache.nifi.provenance.expiration.FileRemovalAction;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriters;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import static org.apache.nifi.provenance.toc.TocUtil.getTocFile;


// TODO: When API, FlowController, and supporting classes are refactored/reimplemented migrate this class and its accompanying imports to minifi package structure
public class MiNiFiPersistentProvenanceRepository implements ProvenanceRepository {

    public static final String EVENT_CATEGORY = "Provenance Repository";
    private static final String FILE_EXTENSION = ".prov";
    private static final String TEMP_FILE_SUFFIX = ".prov.part";
    private static final long PURGE_EVENT_MILLISECONDS = 2500L; //Determines the frequency over which the task to delete old events will occur
    public static final int SERIALIZATION_VERSION = 8;
    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");


    private static final Logger logger = LoggerFactory.getLogger(MiNiFiPersistentProvenanceRepository.class);

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
    private final boolean alwaysSync;
    private final int rolloverCheckMillis;
    private final int maxAttributeChars;

    private final ScheduledExecutorService scheduledExecService;
    private final ScheduledExecutorService rolloverExecutor;

    private final List<ExpirationAction> expirationActions = new ArrayList<>();

    private final AtomicLong writerIndex = new AtomicLong(0L);
    private final AtomicLong storageDirectoryIndex = new AtomicLong(0L);
    private final AtomicLong bytesWrittenSinceRollover = new AtomicLong(0L);
    private final AtomicInteger recordsWrittenSinceRollover = new AtomicInteger(0);
    private final AtomicInteger rolloverCompletions = new AtomicInteger(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final AtomicInteger dirtyWriterCount = new AtomicInteger(0);

    private EventReporter eventReporter;

    private final Lock idLock = new ReentrantLock();
    private Long maxId = null;

    public MiNiFiPersistentProvenanceRepository() throws IOException {
        maxPartitionMillis = 0;
        maxPartitionBytes = 0;
        writers = null;
        configuration = null;
        alwaysSync = false;
        rolloverCheckMillis = 0;
        maxAttributeChars = 0;
        scheduledExecService = null;
        rolloverExecutor = null;
        eventReporter = null;
    }

    public MiNiFiPersistentProvenanceRepository(final NiFiProperties nifiProperties) throws IOException {
        this(createRepositoryConfiguration(nifiProperties), 10000);
    }

    public MiNiFiPersistentProvenanceRepository(final RepositoryConfiguration configuration, final int rolloverCheckMillis) throws IOException {
        if (configuration.getStorageDirectories().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one storage directory");
        }

        this.configuration = configuration;
        this.maxAttributeChars = configuration.getMaxAttributeChars();

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
        this.alwaysSync = configuration.isAlwaysSync();
        this.rolloverCheckMillis = rolloverCheckMillis;

        scheduledExecService = Executors.newScheduledThreadPool(3, new NamedThreadFactory("Provenance Maintenance Thread"));

        // The number of rollover threads is a little bit arbitrary but comes from the idea that multiple storage directories generally
        // live on separate physical partitions. As a result, we want to use at least one thread per partition in order to utilize the
        // disks efficiently. However, the rollover actions can be somewhat CPU intensive, so we double the number of threads in order
        // to account for that.
        final int numRolloverThreads = configuration.getStorageDirectories().size() * 2;
        rolloverExecutor = Executors.newScheduledThreadPool(numRolloverThreads, new NamedThreadFactory("Provenance Repository Rollover Thread"));
    }

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory) throws IOException {
        writeLock.lock();
        try {
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
                                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to roll over Provenance Event Log due to " + e.toString());
                                    }
                                }
                            } finally {
                                writeLock.unlock();
                            }
                        }
                    }
                }, rolloverCheckMillis, rolloverCheckMillis, TimeUnit.MILLISECONDS);

                expirationActions.add(new FileRemovalAction());

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

    private static RepositoryConfiguration createRepositoryConfiguration(final NiFiProperties properties) throws IOException {
        final Map<String, Path> storageDirectories = properties.getProvenanceRepositoryPaths();
        if (storageDirectories.isEmpty()) {
            storageDirectories.put("provenance_repository", Paths.get("provenance_repository"));
        }
        final String storageTime = properties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_TIME, "24 hours");
        final String storageSize = properties.getProperty(NiFiProperties.PROVENANCE_MAX_STORAGE_SIZE, "1 GB");
        final String rolloverTime = properties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_TIME, "5 mins");
        final String rolloverSize = properties.getProperty(NiFiProperties.PROVENANCE_ROLLOVER_SIZE, "100 MB");
        final String shardSize = properties.getProperty(NiFiProperties.PROVENANCE_INDEX_SHARD_SIZE, "500 MB");
        final int journalCount = properties.getIntegerProperty(NiFiProperties.PROVENANCE_JOURNAL_COUNT, 16);

        final long storageMillis = FormatUtils.getTimeDuration(storageTime, TimeUnit.MILLISECONDS);
        final long maxStorageBytes = DataUnit.parseDataSize(storageSize, DataUnit.B).longValue();
        final long rolloverMillis = FormatUtils.getTimeDuration(rolloverTime, TimeUnit.MILLISECONDS);
        final long rolloverBytes = DataUnit.parseDataSize(rolloverSize, DataUnit.B).longValue();

        final boolean compressOnRollover = Boolean.parseBoolean(properties.getProperty(NiFiProperties.PROVENANCE_COMPRESS_ON_ROLLOVER));

        final Boolean alwaysSync = Boolean.parseBoolean(properties.getProperty("nifi.provenance.repository.always.sync", "false"));

        final int defaultMaxAttrChars = 65536;
        final String maxAttrLength = properties.getProperty("nifi.provenance.repository.max.attribute.length", String.valueOf(defaultMaxAttrChars));
        int maxAttrChars;
        try {
            maxAttrChars = Integer.parseInt(maxAttrLength);
            // must be at least 36 characters because that's the length of the uuid attribute,
            // which must be kept intact
            if (maxAttrChars < 36) {
                maxAttrChars = 36;
                logger.warn("Found max attribute length property set to " + maxAttrLength + " but minimum length is 36; using 36 instead");
            }
        } catch (final Exception e) {
            maxAttrChars = defaultMaxAttrChars;
        }

        final RepositoryConfiguration config = new RepositoryConfiguration();
        for (final Path path : storageDirectories.values()) {
            config.addStorageDirectory(path.toFile());
        }
        config.setCompressOnRollover(compressOnRollover);
        config.setMaxEventFileCapacity(rolloverBytes);
        config.setMaxEventFileLife(rolloverMillis, TimeUnit.MILLISECONDS);
        config.setMaxRecordLife(storageMillis, TimeUnit.MILLISECONDS);
        config.setMaxStorageCapacity(maxStorageBytes);
        config.setJournalCount(journalCount);
        config.setMaxAttributeChars(maxAttrChars);

        if (shardSize != null) {
            config.setDesiredIndexSize(DataUnit.parseDataSize(shardSize, DataUnit.B).longValue());
        }

        config.setAlwaysSync(alwaysSync);

        return config;
    }

    // protected in order to override for unit tests
    protected RecordWriter[] createWriters(final RepositoryConfiguration config, final long initialRecordId) throws IOException {
        final List<File> storageDirectories = config.getStorageDirectories();

        final RecordWriter[] writers = new RecordWriter[config.getJournalCount()];
        for (int i = 0; i < config.getJournalCount(); i++) {
            final File storageDirectory = storageDirectories.get(i % storageDirectories.size());
            final File journalDirectory = new File(storageDirectory, "journals");
            final File journalFile = new File(journalDirectory, String.valueOf(initialRecordId) + ".journal." + i);

            writers[i] = RecordWriters.newSchemaRecordWriter(journalFile, false, false);
            writers[i].writeHeader(initialRecordId);
        }

        logger.info("Created new Provenance Event Writers for events starting with ID {}", initialRecordId);
        return writers;
    }

    /**
     * @return the maximum number of characters that any Event attribute should contain. If the event contains
     * more characters than this, the attribute may be truncated on retrieval
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

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final NiFiUser user) throws IOException {
        throw new MethodNotSupportedException("Cannot list events for a specified user.");
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
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
            final long fileFirstId = Long.parseLong(baseName);
            sortedPathMap.put(fileFirstId, file.toPath());

            if (fileFirstId > maxId) {
                maxId = fileFirstId;
                maxIdFile = file;
            }
        }

        if (maxIdFile != null) {
            // Determine the max ID in the last file.
            try (final RecordReader reader = RecordReaders.newRecordReader(maxIdFile, getAllLogFiles(), maxAttributeChars)) {
                final long eventId = reader.getMaxEventId();
                if (eventId > maxId) {
                    maxId = eventId;
                    checkAndSetMaxEventId(maxId);
                }

            } catch (final IOException ioe) {
                logger.error("Failed to read Provenance Event File {} due to {}", maxIdFile, ioe);
                logger.error("", ioe);
            }
        }

        // Establish current max event ID and increment generator to pick up from this point
        checkAndSetMaxEventId(maxId);
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

                final String basename = StringUtils.substringBefore(recoveredJournal.getName(), ".");
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

        logger.info("Recovered records");
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
                            + "This most often happens as a result of the repository running out of disk space or the JMV running out of memory.");
                }

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
                        checkAndSetMaxEventId(eventId);
                    }

                    if (alwaysSync) {
                        writer.sync();
                    }

                    totalJournalSize = bytesWrittenSinceRollover.addAndGet(bytesWritten);
                    recordsWrittenSinceRollover.getAndIncrement();
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
     * @return all of the Provenance Event Log Files (not the journals, the merged files) available across all storage directories.
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
     * @param logFiles   the log files to consider
     * @param timeCutoff if a log file's last modified date is before timeCutoff, it will be skipped
     * @return the size of all log files given whose last mod date comes after (or equal to) timeCutoff
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
                final String baseName1 = StringUtils.substringBefore(o1.getName(), ".");
                final String baseName2 = StringUtils.substringBefore(o2.getName(), ".");

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
        if (bytesUsed > configuration.getMaxStorageCapacity() * 0.9) {
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
            final String baseName = StringUtils.substringBefore(file.getName(), ".");
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
                eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to perform Expiration Action " + currentAction +
                        " on Provenance Event file " + file + " due to " + t.toString() + "; will not perform additional Expiration Actions " +
                        "on this file at this time");
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
                final String baseName = StringUtils.substringBefore(filename, ".");

                if (removed.contains(baseName)) {
                    itr.remove();
                }
            }

            updated = idToPathMap.compareAndSet(existingPathMap, newPathMap);
            logger.debug("After expiration, path map: {}", newPathMap);
        }
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
     * Blocks the calling thread until the repository rolls over. This is intended for unit testing.
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
     * @return the number of journal files that exist across all storage directories
     */
    // made protected for testing purposes
    protected int getJournalCount() {
        // determine how many 'journals' we have in the journals directories
        int journalFileCount = 0;
        for (final File storageDir : configuration.getStorageDirectories()) {
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

    /**
     * <p>
     * MUST be called with the write lock held.
     * </p>
     * <p>
     * Rolls over the data in the journal files, merging them into a single Provenance Event Log File, and
     * compressing as needed.
     *
     * @param force if true, will force a rollover regardless of whether or not data has been written
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
                    logger.debug("Going to merge {} files for journals starting with ID {}", journalsToMerge.size(), StringUtils.substringBefore(journalsToMerge.get(0).getName(), "."));
                }
            }

            // Choose a storage directory to store the merged file in.
            final long storageDirIdx = storageDirectoryIndex.getAndIncrement();
            final List<File> storageDirs = configuration.getStorageDirectories();
            final File storageDir = storageDirs.get((int) (storageDirIdx % storageDirs.size()));

            Future<?> future = null;
            if (!journalsToMerge.isEmpty()) {
                // Run the rollover logic in a background thread.
                final AtomicReference<Future<?>> futureReference = new AtomicReference<>();
                final int recordsWritten = recordsWrittenSinceRollover.getAndSet(0);
                final Runnable rolloverRunnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final File fileRolledOver;

                            try {
                                fileRolledOver = mergeJournals(journalsToMerge, getMergeFile(journalsToMerge, storageDir), eventReporter);
                            } catch (final IOException ioe) {
                                logger.error("Failed to merge Journal Files {} into a Provenance Log File due to {}", journalsToMerge, ioe.toString());
                                logger.error("", ioe);
                                return;
                            }

                            if (fileRolledOver == null) {
                                logger.debug("Couldn't merge journals. Will try again in 10 seconds. journalsToMerge: {}, storageDir: {}", journalsToMerge, storageDir);
                                return;
                            }
                            final File file = fileRolledOver;

                            // update our map of id to Path
                            // We need to make sure that another thread doesn't also update the map at the same time. We cannot
                            // use the write lock when purging old events, and we want to use the same approach here.
                            boolean updated = false;
                            final Long fileFirstEventId = Long.valueOf(StringUtils.substringBefore(fileRolledOver.getName(), "."));
                            while (!updated) {
                                final SortedMap<Long, Path> existingPathMap = idToPathMap.get();
                                final SortedMap<Long, Path> newIdToPathMap = new TreeMap<>(new PathMapComparator());
                                newIdToPathMap.putAll(existingPathMap);
                                newIdToPathMap.put(fileFirstEventId, file.toPath());
                                updated = idToPathMap.compareAndSet(existingPathMap, newIdToPathMap);
                            }

                            logger.info("Successfully Rolled over Provenance Event file containing {} records", recordsWritten);
                            rolloverCompletions.getAndIncrement();

                            // We have finished successfully. Cancel the future so that we don't run anymore
                            Future<?> future;
                            while ((future = futureReference.get()) == null) {
                                try {
                                    Thread.sleep(10L);
                                } catch (final InterruptedException ie) {
                                }
                            }

                            future.cancel(false);
                        } catch (final Throwable t) {
                            logger.error("Failed to rollover Provenance repository due to {}", t.toString());
                            logger.error("", t);
                        }
                    }
                };

                // We are going to schedule the future to run immediately and then repeat every 10 seconds. This allows us to keep retrying if we
                // fail for some reason. When we succeed, the Runnable will cancel itself.
                future = rolloverExecutor.scheduleWithFixedDelay(rolloverRunnable, 0, 10, TimeUnit.SECONDS);
                futureReference.set(future);
            }

            streamStartTime.set(System.currentTimeMillis());
            bytesWrittenSinceRollover.set(0);

            // We don't want to create new 'writers' until the number of unmerged journals falls below our threshold. So we wait
            // here before we repopulate the 'writers' member variable and release the lock.
            int journalFileCount = getJournalCount();
            long repoSize = getSize(getLogFiles(), 0L);
            final int journalCountThreshold = configuration.getJournalCount() * 5;
            final long sizeThreshold = (long) (configuration.getMaxStorageCapacity() * 1.1D); // do not go over 10% of max capacity

            // check if we need to apply backpressure.
            // If we have too many journal files, or if the repo becomes too large, backpressure is necessary. Without it,
            // if the rate at which provenance events are registered exceeds the rate at which we can compress/merge them,
            // then eventually we will end up with all of the data stored in the 'journals' directory. This
            // would mean that the data would never even be accessible. In order to prevent this, if we exceeds 110% of the configured
            // max capacity for the repo, or if we have 5 sets of journal files waiting to be merged, we will block here until
            // that is no longer the case.
            if (journalFileCount > journalCountThreshold || repoSize > sizeThreshold) {
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

                logger.info("Provenance Repository has now caught up with rolling over journal files. Current number of "
                        + "journal files to be rolled over is {}", journalFileCount);
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
                if (journalFile.isDirectory()) {
                    continue;
                }

                final String basename = StringUtils.substringBefore(journalFile.getName(), ".");
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
            final String basename =  StringUtils.substringBefore(journal.getName(), ".");
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

    /**
     * <p>
     * Merges all of the given Journal Files into a single, merged Provenance Event Log File. As these records are merged, they will be compressed, if the repository is configured to compress records
     * </p>
     * <p>
     * <p>
     * If the repository is configured to compress the data, the file written to may not be the same as the <code>suggestedMergeFile</code>, as a filename extension of '.gz' may be appended. If the
     * journals are successfully merged, the file that they were merged into will be returned. If unable to merge the records (for instance, because the repository has been closed or because the list
     * of journal files was empty), this method will return <code>null</code>.
     * </p>
     *
     * @param journalFiles       the journal files to merge
     * @param suggestedMergeFile the file to write the merged records to
     * @param eventReporter      the event reporter to report any warnings or errors to; may be null.
     * @return the file that the given journals were merged into, or <code>null</code> if no records were merged.
     * @throws IOException if a problem occurs writing to the mergedFile, reading from a journal
     */
    File mergeJournals(final List<File> journalFiles, final File suggestedMergeFile, final EventReporter eventReporter) throws IOException {
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
                final String suffix1 = StringUtils.substringAfterLast(o1.getName(), ".");
                final String suffix2 = StringUtils.substringAfterLast(o2.getName(), ".");

                try {
                    final int journalIndex1 = Integer.parseInt(suffix1);
                    final int journalIndex2 = Integer.parseInt(suffix2);
                    return Integer.compare(journalIndex1, journalIndex2);
                } catch (final NumberFormatException nfe) {
                    return o1.getName().compareTo(o2.getName());
                }
            }
        });

        final String firstJournalFile = journalFiles.get(0).getName();
        final String firstFileSuffix = StringUtils.substringAfterLast(firstJournalFile, ".");
        final boolean allPartialFiles = firstFileSuffix.equals("0");

        // check if we have all of the "partial" files for the journal.
        if (allPartialFiles) {
            if (suggestedMergeFile.exists()) {
                // we have all "partial" files and there is already a merged file. Delete the data from the index
                // because the merge file may not be fully merged. We will re-merge.
                logger.warn("Merged Journal File {} already exists; however, all partial journal files also exist "
                        + "so assuming that the merge did not finish. Repeating procedure in order to ensure consistency.");

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
            logger.warn("Cannot merge journal files {} because expected first file to end with extension '.0' "
                    + "but it did not; assuming that the files were already merged but only some finished deletion "
                    + "before restart. Deleting remaining partial journal files.", journalFiles);

            for (final File file : journalFiles) {
                if (!file.delete() && file.exists()) {
                    logger.warn("Failed to delete unneeded journal file {}; this file should be cleaned up manually", file);
                }
            }

            return null;
        }

        final long startNanos = System.nanoTime();

        // Map each journal to a RecordReader
        final List<RecordReader> readers = new ArrayList<>();
        int records = 0;

        final boolean isCompress = configuration.isCompressOnRollover();
        final File writerFile = isCompress ? new File(suggestedMergeFile.getParentFile(), suggestedMergeFile.getName() + ".gz") : suggestedMergeFile;

        try {
            for (final File journalFile : journalFiles) {
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
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "re " + ioe.toString());
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
                } catch (final Exception e) {
                    logger.warn("Failed to generate Provenance Event Record from Journal due to " + e + "; it's possible that the record wasn't "
                            + "completely written to the file. This record will be skipped.");
                    if (logger.isDebugEnabled()) {
                        logger.warn("", e);
                    }

                    if (eventReporter != null) {
                        eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to read Provenance Event Record from Journal due to " + e +
                                "; it's possible that hte record wasn't completely written to the file. This record will be skipped.");
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

            // loop over each entry in the map, persisting the records to the merged file in order, and populating the map
            // with the next entry from the journal file from which the previous record was written.
            try (final RecordWriter writer = RecordWriters.newSchemaRecordWriter(writerFile, configuration.isCompressOnRollover(), true)) {
                writer.writeHeader(minEventId);

                while (!recordToReaderMap.isEmpty()) {
                    final Map.Entry<StandardProvenanceEventRecord, RecordReader> entry = recordToReaderMap.entrySet().iterator().next();
                    final StandardProvenanceEventRecord record = entry.getKey();
                    final RecordReader reader = entry.getValue();

                    writer.writeRecord(record, record.getEventId());
                    final int blockIndex = writer.getTocWriter().getCurrentBlockIndex();

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

        // Success. Remove all of the journal files, as they're no longer needed, now that they've been merged.
        for (final File journalFile : journalFiles) {
            if (!journalFile.delete() && journalFile.exists()) {
                logger.warn("Failed to remove temporary journal file {}; this file should be cleaned up manually", journalFile.getAbsolutePath());

                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to remove temporary journal file " +
                            journalFile.getAbsolutePath() + "; this file should be cleaned up manually");
                }
            }

            final File tocFile = getTocFile(journalFile);
            if (!tocFile.delete() && tocFile.exists()) {
                logger.warn("Failed to remove temporary journal TOC file {}; this file should be cleaned up manually", tocFile.getAbsolutePath());

                if (eventReporter != null) {
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Failed to remove temporary journal TOC file " +
                            tocFile.getAbsolutePath() + "; this file should be cleaned up manually");
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
            logger.info("Successfully merged {} journal files ({} records) into single Provenance Log File {} in {} milliseconds", journalFiles.size(), records, suggestedMergeFile, millis);
        }

        return writerFile;
    }

    private StandardProvenanceEventRecord truncateAttributes(final StandardProvenanceEventRecord original) {
        boolean requireTruncation = false;

        for (final Map.Entry<String, String> entry : original.getAttributes().entrySet()) {
            if (entry.getValue().length() > maxAttributeChars) {
                requireTruncation = true;
                break;
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
            if (entry.getValue().length() > maxAttributeChars) {
                truncatedAttrs.put(entry.getKey(), entry.getValue().substring(0, maxAttributeChars));
            } else {
                truncatedAttrs.put(entry.getKey(), entry.getValue());
            }
        }
        return truncatedAttrs;
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

        if ((dirtyWriterCount.get() > 0) || (writtenSinceRollover > 0 && System.currentTimeMillis() > streamStartTime.get() + maxPartitionMillis)) {
            return true;
        }

        return false;
    }

    /**
     * @return a List of all Provenance Event Log Files, sorted in ascending order by the first Event ID in each file
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

    /**
     * Returns the Event ID of the first event in the given Provenance Event Log File.
     *
     * @param logFile the log file from which to obtain the first Event ID
     * @return the ID of the first event in the given log file
     */
    private long getFirstEventId(final File logFile) {
        final String name = logFile.getName();
        final int dotIndex = name.indexOf(".");
        return Long.parseLong(name.substring(0, dotIndex));
    }

    @Override
    public Long getMaxEventId() {
        idLock.lock();
        try {
            return this.maxId;
        } finally {
            idLock.unlock();
        }
    }

    private void checkAndSetMaxEventId(long id) {
        idLock.lock();
        try {
            if (maxId == null || id > maxId) {
                maxId = id;
            }
        } finally {
            idLock.unlock();
        }
    }

    public Collection<Path> getAllLogFiles() {
        final SortedMap<Long, Path> map = idToPathMap.get();
        return map == null ? new ArrayList<Path>() : map.values();
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

    /* TODO: The following are items to be removed when a new ProvenanceEventRepository interface can be defined and incorporated and deal with querying/indexing that does not apply */
    public static final class MethodNotSupportedException extends RuntimeException {
        public MethodNotSupportedException() {
            super();
        }

        public MethodNotSupportedException(String message) {
            super(message);
        }

        public MethodNotSupportedException(String message, Throwable cause) {
            super(message, cause);
        }

        public MethodNotSupportedException(Throwable cause) {
            super(cause);
        }

        protected MethodNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    @Override
    public QuerySubmission submitQuery(Query query, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Querying and indexing is not available for implementation " + this.getClass().getName());
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(String queryIdentifier, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Querying and indexing is not available for implementation " + this.getClass().getName());
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        throw new MethodNotSupportedException("Querying and indexing is not available for implementation " + this.getClass().getName());
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        throw new MethodNotSupportedException("Querying and indexing is not available for implementation " + this.getClass().getName());
    }

    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Computation of lineage is not available for implementation " + this.getClass().getName());
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(long eventId, NiFiUser user) {
        throw new MethodNotSupportedException("Computation of lineage is not available for implementation " + this.getClass().getName());
    }

    @Override
    public AsyncLineageSubmission submitExpandChildren(final long eventId, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Computation of lineage is not available for implementation " + this.getClass().getName());
    }

    @Override
    public AsyncLineageSubmission submitExpandParents(final long eventId, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Computation of lineage is not available for implementation " + this.getClass().getName());
    }

    @Override
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, NiFiUser niFiUser) {
        throw new MethodNotSupportedException("Computation of lineage is not available for implementation " + this.getClass().getName());
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) throws IOException {
        throw new MethodNotSupportedException("Cannot handle user authorization requests.");
    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }
}
