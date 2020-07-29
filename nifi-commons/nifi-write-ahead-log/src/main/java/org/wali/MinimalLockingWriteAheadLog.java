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
package org.wali;

import static java.util.Objects.requireNonNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.nifi.wali.SequentialAccessWriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This implementation provides as little Locking as possible in order to
 * provide the highest throughput possible. However, this implementation is ONLY
 * appropriate if it can be guaranteed that only a single thread will ever issue
 * updates for a given Record at any one time.
 * </p>
 *
 * @param <T> type of record this WAL is for
 *
 * @deprecated This implementation is now deprecated in favor of {@link SequentialAccessWriteAheadLog}.
 *             This implementation, when given more than 1 partition, can have issues recovering after a sudden loss
 *             of power or an operating system crash.
 */
@Deprecated
public final class MinimalLockingWriteAheadLog<T> implements WriteAheadRepository<T> {

    private final Path basePath;
    private final Path partialPath;
    private final Path snapshotPath;

    private final SerDeFactory<T> serdeFactory;
    private final SyncListener syncListener;
    private final FileChannel lockChannel;
    private final AtomicLong transactionIdGenerator = new AtomicLong(0L);

    private final Partition<T>[] partitions;
    private final AtomicLong partitionIndex = new AtomicLong(0L);
    private final ConcurrentMap<Object, T> recordMap = new ConcurrentHashMap<>();
    private final Map<Object, T> unmodifiableRecordMap = Collections.unmodifiableMap(recordMap);
    private final Set<String> externalLocations = new CopyOnWriteArraySet<>();

    private final Set<String> recoveredExternalLocations = new CopyOnWriteArraySet<>();

    private final AtomicInteger numberBlackListedPartitions = new AtomicInteger(0);

    private static final Logger logger = LoggerFactory.getLogger(MinimalLockingWriteAheadLog.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock(); // required to update a partition
    private final Lock writeLock = rwLock.writeLock(); // required for checkpoint

    private volatile boolean updated = false;
    private volatile boolean recovered = false;

    public MinimalLockingWriteAheadLog(final Path path, final int partitionCount, final SerDe<T> serde, final SyncListener syncListener) throws IOException {
        this(new TreeSet<>(Collections.singleton(path)), partitionCount, new SingletonSerDeFactory<>(serde), syncListener);
    }

    public MinimalLockingWriteAheadLog(final Path path, final int partitionCount, final SerDeFactory<T> serdeFactory, final SyncListener syncListener) throws IOException {
        this(new TreeSet<>(Collections.singleton(path)), partitionCount, serdeFactory, syncListener);
    }

    public MinimalLockingWriteAheadLog(final SortedSet<Path> paths, final int partitionCount, final SerDe<T> serde, final SyncListener syncListener) throws IOException {
        this(paths, partitionCount, new SingletonSerDeFactory<>(serde), syncListener);
    }

    /**
     *
     * @param paths a sorted set of Paths to use for the partitions/journals and
     * the snapshot. The snapshot will always be written to the first path
     * specified.
     * @param partitionCount the number of partitions/journals to use. For best
     * performance, this should be close to the number of threads that are
     * expected to update the repository simultaneously
     * @param serdeFactory the factory for the serializer/deserializer for records
     * @param syncListener the listener
     * @throws IOException if unable to initialize due to IO issue
     */
    @SuppressWarnings("unchecked")
    public MinimalLockingWriteAheadLog(final SortedSet<Path> paths, final int partitionCount, final SerDeFactory<T> serdeFactory, final SyncListener syncListener) throws IOException {
        this.syncListener = syncListener;

        requireNonNull(paths);
        requireNonNull(serdeFactory);

        if (paths.isEmpty()) {
            throw new IllegalArgumentException("Paths must be non-empty");
        }

        int resolvedPartitionCount = partitionCount;
        int existingPartitions = 0;
        for (final Path path : paths) {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }

            final File file = path.toFile();
            if (!file.isDirectory()) {
                throw new IOException("Path given [" + path + "] is not a directory");
            }
            if (!file.canWrite()) {
                throw new IOException("Path given [" + path + "] is not writable");
            }
            if (!file.canRead()) {
                throw new IOException("Path given [" + path + "] is not readable");
            }
            if (!file.canExecute()) {
                throw new IOException("Path given [" + path + "] is not executable");
            }

            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    if (child.isDirectory() && child.getName().startsWith("partition-")) {
                        existingPartitions++;
                    }
                }

                if (existingPartitions != 0 && existingPartitions != partitionCount) {
                    logger.warn("Constructing MinimalLockingWriteAheadLog with partitionCount={}, but the repository currently has "
                            + "{} partitions; ignoring argument and proceeding with {} partitions",
                            new Object[]{partitionCount, existingPartitions, existingPartitions});
                    resolvedPartitionCount = existingPartitions;
                }
            }
        }

        this.basePath = paths.iterator().next();
        this.partialPath = basePath.resolve("snapshot.partial");
        this.snapshotPath = basePath.resolve("snapshot");
        this.serdeFactory = serdeFactory;

        final Path lockPath = basePath.resolve("wali.lock");
        lockChannel = new FileOutputStream(lockPath.toFile()).getChannel();
        lockChannel.lock();

        partitions = new Partition[resolvedPartitionCount];

        Iterator<Path> pathIterator = paths.iterator();
        for (int i = 0; i < resolvedPartitionCount; i++) {
            // If we're out of paths, create a new iterator to start over.
            if (!pathIterator.hasNext()) {
                pathIterator = paths.iterator();
            }

            final Path partitionBasePath = pathIterator.next();

            partitions[i] = new Partition<>(partitionBasePath.resolve("partition-" + i), serdeFactory, i, getVersion());
        }
    }

    @Override
    public int update(final Collection<T> records, final boolean forceSync) throws IOException {
        if (!recovered) {
            throw new IllegalStateException("Cannot update repository until record recovery has been performed");
        }

        if (records.isEmpty()) {
            return -1;
        }

        updated = true;
        readLock.lock();
        try {
            while (true) {
                final int numBlackListed = numberBlackListedPartitions.get();
                if (numBlackListed >= partitions.length) {
                    throw new IOException("All Partitions have been blacklisted due to "
                            + "failures when attempting to update. If the Write-Ahead Log is able to perform a checkpoint, "
                            + "this issue may resolve itself. Otherwise, manual intervention will be required.");
                }

                final long partitionIdx = partitionIndex.getAndIncrement();
                final int resolvedIdx = (int) (partitionIdx % partitions.length);
                final Partition<T> partition = partitions[resolvedIdx];
                if (partition.tryClaim()) {
                    try {
                        final long transactionId = transactionIdGenerator.getAndIncrement();
                        if (logger.isTraceEnabled()) {
                            for (final T record : records) {
                                logger.trace("Partition {} performing Transaction {}: {}", new Object[] {partition, transactionId, record});
                            }
                        }

                        try {
                            partition.update(records, transactionId, unmodifiableRecordMap, forceSync);
                        } catch (final Throwable t) {
                            partition.blackList();
                            numberBlackListedPartitions.incrementAndGet();
                            throw t;
                        }

                        if (forceSync && syncListener != null) {
                            syncListener.onSync(resolvedIdx);
                        }
                    } finally {
                        partition.releaseClaim();
                    }

                    for (final T record : records) {
                        final UpdateType updateType = serdeFactory.getUpdateType(record);
                        final Object recordIdentifier = serdeFactory.getRecordIdentifier(record);

                        if (updateType == UpdateType.DELETE) {
                            recordMap.remove(recordIdentifier);
                        } else if (updateType == UpdateType.SWAP_OUT) {
                            final String newLocation = serdeFactory.getLocation(record);
                            if (newLocation == null) {
                                logger.error("Received Record (ID=" + recordIdentifier + ") with UpdateType of SWAP_OUT but "
                                        + "no indicator of where the Record is to be Swapped Out to; these records may be "
                                        + "lost when the repository is restored!");
                            } else {
                                recordMap.remove(recordIdentifier);
                                this.externalLocations.add(newLocation);
                            }
                        } else if (updateType == UpdateType.SWAP_IN) {
                            final String newLocation = serdeFactory.getLocation(record);
                            if (newLocation == null) {
                                logger.error("Received Record (ID=" + recordIdentifier + ") with UpdateType of SWAP_IN but no "
                                        + "indicator of where the Record is to be Swapped In from; these records may be duplicated "
                                        + "when the repository is restored!");
                            } else {
                                externalLocations.remove(newLocation);
                            }
                            recordMap.put(recordIdentifier, record);
                        } else {
                            recordMap.put(recordIdentifier, record);
                        }
                    }

                    return resolvedIdx;
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<T> recoverRecords() throws IOException {
        if (updated) {
            throw new IllegalStateException("Cannot recover records after updating the repository; must call recoverRecords first");
        }

        final long recoverStart = System.nanoTime();
        writeLock.lock();
        try {
            Long maxTransactionId = recoverFromSnapshot(recordMap);
            recoverFromEdits(recordMap, maxTransactionId);

            for (final Partition<T> partition : partitions) {
                final long transId = partition.getMaxRecoveredTransactionId();
                if (maxTransactionId == null || transId > maxTransactionId) {
                    maxTransactionId = transId;
                }
            }

            this.transactionIdGenerator.set(maxTransactionId + 1);
            this.externalLocations.addAll(recoveredExternalLocations);
            logger.info("{} finished recovering records. Performing Checkpoint to ensure proper state of Partitions before updates", this);
        } finally {
            writeLock.unlock();
        }
        final long recoverNanos = System.nanoTime() - recoverStart;
        final long recoveryMillis = TimeUnit.MILLISECONDS.convert(recoverNanos, TimeUnit.NANOSECONDS);
        logger.info("Successfully recovered {} records in {} milliseconds", recordMap.size(), recoveryMillis);
        checkpoint();

        recovered = true;
        return recordMap.values();
    }

    @Override
    public Set<String> getRecoveredSwapLocations() throws IOException {
        return recoveredExternalLocations;
    }

    private Long recoverFromSnapshot(final Map<Object, T> recordMap) throws IOException {
        final boolean partialExists = Files.exists(partialPath);
        final boolean snapshotExists = Files.exists(snapshotPath);

        if (!partialExists && !snapshotExists) {
            return null;
        }

        if (partialExists && snapshotExists) {
            // both files exist -- assume we failed while checkpointing. Delete
            // the partial file
            Files.delete(partialPath);
        } else if (partialExists) {
            // partial exists but snapshot does not -- we must have completed
            // creating the partial, deleted the snapshot
            // but crashed before renaming the partial to the snapshot. Just
            // rename partial to snapshot
            Files.move(partialPath, snapshotPath);
        }

        if (Files.size(snapshotPath) == 0) {
            logger.warn("{} Found 0-byte Snapshot file; skipping Snapshot file in recovery", this);
            return null;
        }

        // at this point, we know the snapshotPath exists because if it didn't, then we either returned null
        // or we renamed partialPath to snapshotPath. So just Recover from snapshotPath.
        try (final DataInputStream dataIn = new DataInputStream(new BufferedInputStream(Files.newInputStream(snapshotPath, StandardOpenOption.READ)))) {
            final String waliImplementationClass = dataIn.readUTF();
            final int waliImplementationVersion = dataIn.readInt();

            if (!waliImplementationClass.equals(MinimalLockingWriteAheadLog.class.getName())) {
                throw new IOException("Write-Ahead Log located at " + snapshotPath + " was written using the "
                        + waliImplementationClass + " class; cannot restore using " + getClass().getName());
            }

            if (waliImplementationVersion > getVersion()) {
                throw new IOException("Write-Ahead Log located at " + snapshotPath + " was written using version "
                        + waliImplementationVersion + " of the " + waliImplementationClass + " class; cannot restore using Version " + getVersion());
            }

            final String serdeEncoding = dataIn.readUTF(); // ignore serde class name for now
            final int serdeVersion = dataIn.readInt();
            final long maxTransactionId = dataIn.readLong();
            final int numRecords = dataIn.readInt();

            final SerDe<T> serde = serdeFactory.createSerDe(serdeEncoding);
            serde.readHeader(dataIn);

            for (int i = 0; i < numRecords; i++) {
                final T record = serde.deserializeRecord(dataIn, serdeVersion);
                if (record == null) {
                    throw new EOFException();
                }

                final UpdateType updateType = serde.getUpdateType(record);
                if (updateType == UpdateType.DELETE) {
                    logger.warn("While recovering from snapshot, found record with type 'DELETE'; this record will not be restored");
                    continue;
                }

                logger.trace("Recovered from snapshot: {}", record);
                recordMap.put(serde.getRecordIdentifier(record), record);
            }

            final int numSwapRecords = dataIn.readInt();
            final Set<String> swapLocations = new HashSet<>();
            for (int i = 0; i < numSwapRecords; i++) {
                swapLocations.add(dataIn.readUTF());
            }
            this.recoveredExternalLocations.addAll(swapLocations);

            logger.debug("{} restored {} Records and {} Swap Files from Snapshot, ending with Transaction ID {}",
                    new Object[]{this, numRecords, recoveredExternalLocations.size(), maxTransactionId});
            return maxTransactionId;
        }
    }

    /**
     * Recovers records from the edit logs via the Partitions. Returns a boolean
     * if recovery of a Partition requires the Write-Ahead Log be checkpointed
     * before modification.
     *
     * @param modifiableRecordMap map
     * @param maxTransactionIdRestored index of max restored transaction
     * @throws IOException if unable to recover from edits
     */
    private void recoverFromEdits(final Map<Object, T> modifiableRecordMap, final Long maxTransactionIdRestored) throws IOException {
        final Map<Object, T> updateMap = new HashMap<>();
        final Map<Object, T> unmodifiableRecordMap = Collections.unmodifiableMap(modifiableRecordMap);
        final Map<Object, T> ignorableMap = new HashMap<>();
        final Set<String> ignorableSwapLocations = new HashSet<>();

        // populate a map of the next transaction id for each partition to the
        // partition that has that next transaction id.
        final SortedMap<Long, Partition<T>> transactionMap = new TreeMap<>();
        for (final Partition<T> partition : partitions) {
            Long transactionId;
            boolean keepTransaction;
            do {
                transactionId = partition.getNextRecoverableTransactionId();

                keepTransaction = transactionId == null || maxTransactionIdRestored == null || transactionId > maxTransactionIdRestored;
                if (keepTransaction && transactionId != null) {
                    // map this transaction id to its partition so that we can
                    // start restoring transactions from this partition,
                    // starting at 'transactionId'
                    transactionMap.put(transactionId, partition);
                } else if (transactionId != null) {
                    // skip the next transaction, because our snapshot already
                    // contained this transaction.
                    try {
                        partition.recoverNextTransaction(ignorableMap, updateMap, ignorableSwapLocations);
                    } catch (final EOFException e) {
                        logger.error("{} unexpectedly reached End of File while reading from {} for Transaction {}; "
                                + "assuming crash and ignoring this transaction.",
                                new Object[]{this, partition, transactionId});
                    }
                }
            } while (!keepTransaction);
        }

        while (!transactionMap.isEmpty()) {
            final Map.Entry<Long, Partition<T>> firstEntry = transactionMap.entrySet().iterator().next();
            final Long firstTransactionId = firstEntry.getKey();
            final Partition<T> nextPartition = firstEntry.getValue();

            try {
                updateMap.clear();
                final Set<Object> idsRemoved = nextPartition.recoverNextTransaction(unmodifiableRecordMap, updateMap, recoveredExternalLocations);
                modifiableRecordMap.putAll(updateMap);
                for (final Object id : idsRemoved) {
                    modifiableRecordMap.remove(id);
                }
            } catch (final EOFException e) {
                logger.error("{} unexpectedly reached End-of-File when reading from {} for Transaction ID {}; "
                        + "assuming crash and ignoring this transaction",
                        new Object[]{this, nextPartition, firstTransactionId});
            }

            transactionMap.remove(firstTransactionId);

            Long subsequentTransactionId = null;
            try {
                subsequentTransactionId = nextPartition.getNextRecoverableTransactionId();
            } catch (final IOException e) {
                logger.error("{} unexpectedly found End-of-File when reading from {} for Transaction ID {}; "
                        + "assuming crash and ignoring this transaction",
                        new Object[]{this, nextPartition, firstTransactionId});
            }

            if (subsequentTransactionId != null) {
                transactionMap.put(subsequentTransactionId, nextPartition);
            }
        }

        for (final Partition<T> partition : partitions) {
            partition.endRecovery();
        }
    }

    @Override
    public synchronized int checkpoint() throws IOException {
        final Set<T> records;
        final Set<String> swapLocations;
        final long maxTransactionId;

        final long startNanos = System.nanoTime();

        FileOutputStream fileOut = null;
        DataOutputStream dataOut = null;

        long stopTheWorldNanos = -1L;
        long stopTheWorldStart = -1L;
        try {
            final List<OutputStream> partitionStreams = new ArrayList<>();

            writeLock.lock();
            try {
                stopTheWorldStart = System.nanoTime();
                // stop the world while we make a copy of the records that must
                // be checkpointed and rollover the partitions.
                // We copy the records because serializing them is potentially
                // very expensive, especially when we have hundreds
                // of thousands or even millions of them. We don't want to
                // prevent WALI from being used during this time.

                // So the design is to copy all of the records, determine the
                // last transaction ID that the records represent,
                // and roll over the partitions to new write-ahead logs.
                // Then, outside of the write lock, we will serialize the data
                // to disk, and then remove the old Partition data.
                records = new HashSet<>(recordMap.values());
                maxTransactionId = transactionIdGenerator.get() - 1;

                swapLocations = new HashSet<>(externalLocations);
                for (final Partition<T> partition : partitions) {
                    try {
                        partitionStreams.add(partition.rollover());
                    } catch (final Throwable t) {
                        partition.blackList();
                        numberBlackListedPartitions.getAndIncrement();
                        throw t;
                    }
                }
            } finally {
                writeLock.unlock();
            }

            stopTheWorldNanos = System.nanoTime() - stopTheWorldStart;

            // Close all of the Partitions' Output Streams. We do this here, instead of in Partition.rollover()
            // because we want to do this outside of the write lock. Because calling close() on FileOutputStream can
            // be very expensive, as it has to flush the data to disk, we don't want to prevent other Process Sessions
            // from getting committed. Since rollover() transitions the partition to write to a new file already, there
            // is no reason that we need to close this FileOutputStream before releasing the write lock. Also, if any Exception
            // does get thrown when calling close(), we don't need to blacklist the partition, as the stream that was getting
            // closed is not the stream being written to for the partition anyway. We also catch any IOException and wait until
            // after we've attempted to close all streams before we throw an Exception, to avoid resource leaks if one of them
            // is unable to be closed (due to out of storage space, for instance).
            IOException failure = null;
            for (final OutputStream partitionStream : partitionStreams) {
                try {
                    partitionStream.close();
                } catch (final IOException e) {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }

            // notify global sync with the write lock held. We do this because we don't want the repository to get updated
            // while the listener is performing its necessary tasks
            if (syncListener != null) {
                syncListener.onGlobalSync();
            }

            final SerDe<T> serde = serdeFactory.createSerDe(null);

            // perform checkpoint, writing to .partial file
            fileOut = new FileOutputStream(partialPath.toFile());
            dataOut = new DataOutputStream(new BufferedOutputStream(fileOut));
            dataOut.writeUTF(MinimalLockingWriteAheadLog.class.getName());
            dataOut.writeInt(getVersion());
            dataOut.writeUTF(serde.getClass().getName());
            dataOut.writeInt(serde.getVersion());
            dataOut.writeLong(maxTransactionId);
            dataOut.writeInt(records.size());
            serde.writeHeader(dataOut);

            for (final T record : records) {
                logger.trace("Checkpointing {}", record);
                serde.serializeRecord(record, dataOut);
            }

            dataOut.writeInt(swapLocations.size());
            for (final String swapLocation : swapLocations) {
                dataOut.writeUTF(swapLocation);
            }
        } finally {
            if (dataOut != null) {
                try {
                    try {
                        dataOut.flush();
                        fileOut.getFD().sync();
                    } finally {
                        dataOut.close();
                    }
                } catch (final IOException e) {
                    logger.warn("Failed to close Data Stream due to {}", e.toString(), e);
                }
            }
        }

        // delete the snapshot, if it exists, and rename the .partial to
        // snapshot
        Files.deleteIfExists(snapshotPath);
        Files.move(partialPath, snapshotPath);

        // clear all of the edit logs
        final long partitionStart = System.nanoTime();
        for (final Partition<T> partition : partitions) {
            // we can call clearOld without claiming the partition because it
            // does not change the partition's state
            // and the only member variable it touches cannot be modified, other
            // than when #rollover() is called.
            // And since this method is the only one that calls #rollover() and
            // this method is synchronized,
            // the value of that member variable will not change. And it's
            // volatile, so we will get the correct value.
            partition.clearOld();
        }
        final long partitionEnd = System.nanoTime();
        numberBlackListedPartitions.set(0);

        final long endNanos = System.nanoTime();
        final long millis = TimeUnit.MILLISECONDS.convert(endNanos - startNanos, TimeUnit.NANOSECONDS);
        final long partitionMillis = TimeUnit.MILLISECONDS.convert(partitionEnd - partitionStart, TimeUnit.NANOSECONDS);
        final long stopTheWorldMillis = TimeUnit.NANOSECONDS.toMillis(stopTheWorldNanos);

        logger.info("{} checkpointed with {} Records and {} Swap Files in {} milliseconds (Stop-the-world "
                + "time = {} milliseconds, Clear Edit Logs time = {} millis), max Transaction ID {}",
                new Object[]{this, records.size(), swapLocations.size(), millis, stopTheWorldMillis, partitionMillis, maxTransactionId});

        return records.size();
    }

    @Override
    public void shutdown() throws IOException {
        writeLock.lock();
        try {
            for (final Partition<T> partition : partitions) {
                partition.close();
            }
        } finally {
            writeLock.unlock();
            lockChannel.close();

            final File lockFile = new File(basePath.toFile(), "wali.lock");
            lockFile.delete();
        }
    }

    public int getVersion() {
        return 1;
    }

    /**
     * Represents a partition of this repository, which maps directly to a
     * .journal file.
     *
     * All methods with the exceptions of {@link #claim()}, {@link #tryClaim()},
     * and {@link #releaseClaim()} in this Partition MUST be called while
     * holding the claim (via {@link #claim} or {@link #tryClaim()}).
     *
     * @param <S> type of record held in the partitions
     */
    private static class Partition<S> {
        public static final String JOURNAL_EXTENSION = ".journal";
        private static final int NUL_BYTE = 0;
        private static final Pattern JOURNAL_FILENAME_PATTERN = Pattern.compile("\\d+\\.journal");

        private final SerDeFactory<S> serdeFactory;
        private SerDe<S> serde;

        private final Path editDirectory;
        private final int writeAheadLogVersion;

        private DataOutputStream dataOut = null;
        private FileOutputStream fileOut = null;
        private volatile boolean blackListed = false;
        private volatile boolean closed = false;
        private DataInputStream recoveryIn;
        private int recoveryVersion;
        private String currentJournalFilename = "";

        private static final byte TRANSACTION_CONTINUE = 1;
        private static final byte TRANSACTION_COMMIT = 2;

        private final String description;
        private final AtomicLong maxTransactionId = new AtomicLong(-1L);
        private final Logger logger = LoggerFactory.getLogger(MinimalLockingWriteAheadLog.class);

        private final Queue<Path> recoveryFiles;

        public Partition(final Path path, final SerDeFactory<S> serdeFactory, final int partitionIndex, final int writeAheadLogVersion) throws IOException {
            this.editDirectory = path;
            this.serdeFactory = serdeFactory;

            final File file = path.toFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Could not create directory " + file.getAbsolutePath());
            }

            this.recoveryFiles = new LinkedBlockingQueue<>();
            for (final Path recoveryPath : getRecoveryPaths()) {
                recoveryFiles.add(recoveryPath);
            }

            this.description = "Partition-" + partitionIndex;
            this.writeAheadLogVersion = writeAheadLogVersion;
        }

        public boolean tryClaim() {
            return !blackListed;
        }

        public void releaseClaim() {
        }

        public void close() {
            this.closed = true;

            // Note that here we are closing fileOut and NOT dataOut.
            // This is very much intentional, not an oversight. This is done because of
            // the way that the OutputStreams are structured. dataOut wraps a BufferedOutputStream,
            // which then wraps the FileOutputStream. If we close 'dataOut', then this will call
            // the flush() method of BufferedOutputStream. Under normal conditions, this is fine.
            // However, there is a very important corner case to consider:
            //
            //      If we are writing to the DataOutputStream in the update() method and that
            //      call to write() then results in the BufferedOutputStream calling flushBuffer() -
            //      or if we finish the call to update() and call flush() ourselves - it is possible
            //      that the internal buffer of the BufferedOutputStream can get partially written to
            //      to the FileOutputStream and then an IOException occurs. If this occurs, we have
            //      written a partial record to disk. This still is okay, as we have logic to handle
            //      the condition where we have a partial record and then an unexpected End-of-File.
            //      But if we then call close() on 'dataOut', this will call the flush() method of the
            //      underlying BufferedOutputStream. As a result, we will end up again writing the internal
            //      buffer of the BufferedOutputStream to the underlying file. At this point, we are left
            //      not with an unexpected/premature End-of-File but instead a bunch of seemingly random
            //      bytes that happened to be residing in that internal buffer, and this will result in
            //      a corrupt and unrecoverable Write-Ahead Log.
            //
            // Additionally, we are okay not ever calling close on the wrapping BufferedOutputStream and
            // DataOutputStream because they don't actually hold any resources that need to be reclaimed,
            // and after each update to the Write-Ahead Log, we call flush() ourselves to ensure that we don't
            // leave arbitrary data in the BufferedOutputStream that hasn't been flushed to the underlying
            // FileOutputStream.
            final OutputStream out = fileOut;
            if (out != null) {
                try {
                    out.close();
                } catch (final Exception e) {
                }
            }

            this.dataOut = null;
            this.fileOut = null;
        }

        public void blackList() {
            blackListed = true;
            logger.debug("Blacklisted {}", this);
        }

        /**
         * Closes resources pointing to the current journal and begins writing
         * to a new one
         *
         * @throws IOException if failure to rollover
         */
        public OutputStream rollover() throws IOException {
            // Note that here we are closing fileOut and NOT dataOut. See the note in the close()
            // method to understand the logic behind this.
            final OutputStream oldOutputStream = fileOut;
            dataOut = null;
            fileOut = null;

            this.serde = serdeFactory.createSerDe(null);
            final Path editPath = getNewEditPath();
            final FileOutputStream fos = new FileOutputStream(editPath.toFile());
            try {
                final DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(fos));
                outStream.writeUTF(MinimalLockingWriteAheadLog.class.getName());
                outStream.writeInt(writeAheadLogVersion);
                outStream.writeUTF(serde.getClass().getName());
                outStream.writeInt(serde.getVersion());
                serde.writeHeader(outStream);

                outStream.flush();
                dataOut = outStream;
                fileOut = fos;
            } catch (final IOException ioe) {
                try {
                    oldOutputStream.close();
                } catch (final IOException ioe2) {
                    ioe.addSuppressed(ioe2);
                }

                logger.error("Failed to create new journal for {} due to {}", new Object[] {this, ioe.toString()}, ioe);
                try {
                    fos.close();
                } catch (final IOException innerIOE) {
                }

                dataOut = null;
                fileOut = null;
                blackList();

                throw ioe;
            }

            currentJournalFilename = editPath.toFile().getName();

            blackListed = false;
            return oldOutputStream;
        }

        private long getJournalIndex(final File file) {
            final String filename = file.getName();
            final int dotIndex = filename.indexOf(".");
            final String number = filename.substring(0, dotIndex);
            return Long.parseLong(number);
        }

        private Path getNewEditPath() {
            final List<Path> recoveryPaths = getRecoveryPaths();
            final long newIndex;
            if (recoveryPaths == null || recoveryPaths.isEmpty()) {
                newIndex = 1;
            } else {
                final long lastFileIndex = getJournalIndex(recoveryPaths.get(recoveryPaths.size() - 1).toFile());
                newIndex = lastFileIndex + 1;
            }

            return editDirectory.resolve(newIndex + JOURNAL_EXTENSION);
        }

        private List<Path> getRecoveryPaths() {
            final List<Path> paths = new ArrayList<>();

            final File directory = editDirectory.toFile();
            final File[] partitionFiles = directory.listFiles();
            if (partitionFiles == null) {
                return paths;
            }

            for (final File file : partitionFiles) {
                // if file is a journal file but no data has yet been persisted, it may
                // very well be a 0-byte file (the journal is not SYNC'ed to disk after
                // a header is written out, so it may be lost). In this case, the journal
                // is empty, so we can just skip it.
                if (file.isDirectory() || file.length() == 0L) {
                    continue;
                }

                if (!JOURNAL_FILENAME_PATTERN.matcher(file.getName()).matches()) {
                    continue;
                }

                if (isJournalFile(file)) {
                    paths.add(file.toPath());
                } else {
                    logger.warn("Found file {}, but could not access it, or it was not in the expected format; "
                            + "will ignore this file", file.getAbsolutePath());
                }
            }

            // Sort journal files by the numeric portion of the filename
            Collections.sort(paths, new Comparator<Path>() {
                @Override
                public int compare(final Path o1, final Path o2) {
                    if (o1 == null && o2 == null) {
                        return 0;
                    }
                    if (o1 == null) {
                        return 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }

                    final long index1 = getJournalIndex(o1.toFile());
                    final long index2 = getJournalIndex(o2.toFile());
                    return Long.compare(index1, index2);
                }
            });

            return paths;
        }

        void clearOld() {
            final List<Path> oldRecoveryFiles = getRecoveryPaths();

            for (final Path path : oldRecoveryFiles) {
                final File file = path.toFile();
                if (file.getName().equals(currentJournalFilename)) {
                    continue;
                }
                if (file.exists()) {
                    file.delete();
                }
            }
        }

        private boolean isJournalFile(final File file) {
            final String expectedStartsWith = MinimalLockingWriteAheadLog.class.getName();
            try {
                try (final FileInputStream fis = new FileInputStream(file);
                        final InputStream bufferedIn = new BufferedInputStream(fis);
                        final DataInputStream in = new DataInputStream(bufferedIn)) {
                    final String waliImplClassName = in.readUTF();
                    if (!expectedStartsWith.equals(waliImplClassName)) {
                        return false;
                    }
                }
            } catch (final IOException e) {
                return false;
            }

            return true;
        }

        public void update(final Collection<S> records, final long transactionId, final Map<Object, S> recordMap, final boolean forceSync) throws IOException {
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
                final DataOutputStream out = new DataOutputStream(baos)) {

                out.writeLong(transactionId);
                final int numEditsToSerialize = records.size();
                int editsSerialized = 0;
                for (final S record : records) {
                    final Object recordId = serde.getRecordIdentifier(record);
                    final S previousVersion = recordMap.get(recordId);

                    serde.serializeEdit(previousVersion, record, out);
                    if (++editsSerialized < numEditsToSerialize) {
                        out.write(TRANSACTION_CONTINUE);
                    } else {
                        out.write(TRANSACTION_COMMIT);
                    }
                }

                out.flush();

                if (this.closed) {
                    throw new IllegalStateException("Partition is closed");
                }

                baos.writeTo(dataOut);
                dataOut.flush();

                if (forceSync) {
                    synchronized (fileOut) {
                        fileOut.getFD().sync();
                    }
                }
            }
        }

        private DataInputStream createDataInputStream(final Path path) throws IOException {
            return new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private DataInputStream getRecoveryStream() throws IOException {
            if (recoveryIn != null && hasMoreData(recoveryIn)) {
                return recoveryIn;
            }

            while (true) {
                final Path nextRecoveryPath = recoveryFiles.poll();
                if (nextRecoveryPath == null) {
                    return null;
                }

                logger.debug("{} recovering from {}", this, nextRecoveryPath);
                recoveryIn = createDataInputStream(nextRecoveryPath);
                if (hasMoreData(recoveryIn)) {
                    try {
                        final String waliImplementationClass = recoveryIn.readUTF();
                        if (!MinimalLockingWriteAheadLog.class.getName().equals(waliImplementationClass)) {
                            continue;
                        }

                        final long waliVersion = recoveryIn.readInt();
                        if (waliVersion > writeAheadLogVersion) {
                            throw new IOException("Cannot recovery from file " + nextRecoveryPath + " because it was written using "
                                + "WALI version " + waliVersion + ", but the version used to restore it is only " + writeAheadLogVersion);
                        }

                        final String serdeEncoding = recoveryIn.readUTF();
                        this.recoveryVersion = recoveryIn.readInt();
                        serde = serdeFactory.createSerDe(serdeEncoding);

                        serde.readHeader(recoveryIn);
                        break;
                    } catch (final Exception e) {
                        logger.warn("Failed to recover data from Write-Ahead Log for {} because the header information could not be read properly. "
                            + "This often is the result of the file not being fully written out before the application is restarted. This file will be ignored.", nextRecoveryPath);
                    }
                }
            }

            return recoveryIn;
        }

        public Long getNextRecoverableTransactionId() throws IOException {
            while (true) {
                DataInputStream recoveryStream = getRecoveryStream();
                if (recoveryStream == null) {
                    return null;
                }

                final long transactionId;
                try {
                    transactionId = recoveryIn.readLong();
                } catch (final EOFException e) {
                    continue;
                } catch (final Exception e) {
                    // If the stream consists solely of NUL bytes, then we want to treat it
                    // the same as an EOF because we see this happen when we suddenly lose power
                    // while writing to a file.
                    if (remainingBytesAllNul(recoveryIn)) {
                        logger.warn("Failed to recover data from Write-Ahead Log Partition because encountered trailing NUL bytes. "
                            + "This will sometimes happen after a sudden power loss. The rest of this journal file will be skipped for recovery purposes.");
                        continue;
                    } else {
                        throw e;
                    }
                }

                this.maxTransactionId.set(transactionId);
                return transactionId;
            }
        }

        /**
         * In the case of a sudden power loss, it is common - at least in a Linux journaling File System -
         * that the partition file that is being written to will have many trailing "NUL bytes" (0's).
         * If this happens, then on restart we want to treat this as an incomplete transaction, so we detect
         * this case explicitly.
         *
         * @param in the input stream to scan
         * @return <code>true</code> if the InputStream contains no data or contains only NUL bytes
         * @throws IOException if unable to read from the given InputStream
         */
        private boolean remainingBytesAllNul(final InputStream in) throws IOException {
            int nextByte;
            while ((nextByte = in.read()) != -1) {
                if (nextByte != NUL_BYTE) {
                    return false;
                }
            }

            return true;
        }

        private boolean hasMoreData(final InputStream in) throws IOException {
            in.mark(1);
            final int nextByte = in.read();
            in.reset();
            return nextByte >= 0;
        }

        public void endRecovery() throws IOException {
            if (recoveryIn != null) {
                recoveryIn.close();
            }

            final Path nextRecoveryPath = this.recoveryFiles.poll();
            if (nextRecoveryPath != null) {
                throw new IllegalStateException("Signaled to end recovery, but there are more recovery files for Partition "
                        + "in directory " + editDirectory);
            }

            final Path newEditPath = getNewEditPath();

            this.serde = serdeFactory.createSerDe(null);
            final FileOutputStream fos = new FileOutputStream(newEditPath.toFile());
            final DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(fos));
            outStream.writeUTF(MinimalLockingWriteAheadLog.class.getName());
            outStream.writeInt(writeAheadLogVersion);
            outStream.writeUTF(serde.getClass().getName());
            outStream.writeInt(serde.getVersion());
            serde.writeHeader(outStream);

            outStream.flush();
            dataOut = outStream;
            fileOut = fos;
        }

        public Set<Object> recoverNextTransaction(final Map<Object, S> currentRecordMap, final Map<Object, S> updatedRecordMap, final Set<String> swapLocations) throws IOException {
            final Set<Object> idsRemoved = new HashSet<>();

            int transactionFlag;
            do {
                final S record;
                try {
                    record = serde.deserializeEdit(recoveryIn, currentRecordMap, recoveryVersion);
                    if (record == null) {
                        throw new EOFException();
                    }
                } catch (final EOFException eof) {
                    throw eof;
                } catch (final Exception e) {
                    // If the stream consists solely of NUL bytes, then we want to treat it
                    // the same as an EOF because we see this happen when we suddenly lose power
                    // while writing to a file. We also have logic already in the caller of this
                    // method to properly handle EOFException's, so we will simply throw an EOFException
                    // ourselves. However, if that is not the case, then something else has gone wrong.
                    // In such a case, there is not much that we can do. If we simply skip over the transaction,
                    // then the transaction may be indicating that a new attribute was added or changed. Or the
                    // content of the FlowFile changed. A subsequent transaction for the same FlowFile may then
                    // update the connection that is holding the FlowFile. In this case, if we simply skip over
                    // the transaction, we end up with a FlowFile in a queue that has the wrong attributes or
                    // content, and that can result in some very bad behavior - even security vulnerabilities if
                    // a Route processor, for instance, routes incorrectly due to a missing attribute or content
                    // is pointing to a previous claim where sensitive values have not been removed, etc. So
                    // instead of attempting to skip the transaction and move on, we instead just throw the Exception
                    // indicating that the write-ahead log is corrupt and allow the user to handle it as he/she sees
                    // fit (likely this will result in deleting the repo, but it's possible that it could be repaired
                    // manually or through some sort of script).
                    if (remainingBytesAllNul(recoveryIn)) {
                        final EOFException eof = new EOFException("Failed to recover data from Write-Ahead Log Partition because encountered trailing NUL bytes. "
                            + "This will sometimes happen after a sudden power loss. The rest of this journal file will be skipped for recovery purposes.");
                        eof.addSuppressed(e);
                        throw eof;
                    } else {
                        throw e;
                    }
                }


                if (logger.isDebugEnabled()) {
                    logger.debug("{} Recovering Transaction {}: {}", new Object[] { this, maxTransactionId.get(), record });
                }

                final Object recordId = serde.getRecordIdentifier(record);
                final UpdateType updateType = serde.getUpdateType(record);
                if (updateType == UpdateType.DELETE) {
                    updatedRecordMap.remove(recordId);
                    idsRemoved.add(recordId);
                } else if (updateType == UpdateType.SWAP_IN) {
                    final String location = serde.getLocation(record);
                    if (location == null) {
                        logger.error("Recovered SWAP_IN record from edit log, but it did not contain a Location; skipping record");
                    } else {
                        swapLocations.remove(location);
                        updatedRecordMap.put(recordId, record);
                        idsRemoved.remove(recordId);
                    }
                } else if (updateType == UpdateType.SWAP_OUT) {
                    final String location = serde.getLocation(record);
                    if (location == null) {
                        logger.error("Recovered SWAP_OUT record from edit log, but it did not contain a Location; skipping record");
                    } else {
                        swapLocations.add(location);
                        updatedRecordMap.remove(recordId);
                        idsRemoved.add(recordId);
                    }
                } else {
                    updatedRecordMap.put(recordId, record);
                    idsRemoved.remove(recordId);
                }

                transactionFlag = recoveryIn.read();
            } while (transactionFlag != TRANSACTION_COMMIT);

            return idsRemoved;
        }

        /**
         * Must be called after recovery has finished
         *
         * @return max recovered transaction id
         */
        public long getMaxRecoveredTransactionId() {
            return maxTransactionId.get();
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
