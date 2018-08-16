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
package org.apache.nifi.rocksdb;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.StringUtils;
import org.rocksdb.AccessHint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * This RocksDB helper class, instead of forcing to disk every time it's given a record,
 * persists all waiting records on a regular interval (using a ScheduledExecutorService).
 * Like when a metronome ticks.
 */

public class RocksDBMetronome implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetronome.class);

    static final String CONFIGURATION_FAMILY = "configuration.column.family";
    static final String DEFAULT_FAMILY = "default";

    private final AtomicLong lastSyncWarningNanos = new AtomicLong(0L);
    private final int parallelThreads;
    private final int maxWriteBufferNumber;
    private final int minWriteBufferNumberToMerge;
    private final long writeBufferSize;
    private final long maxTotalWalSize;
    private final long delayedWriteRate;
    private final int level0SlowdownWritesTrigger;
    private final int level0StopWritesTrigger;
    private final int maxBackgroundFlushes;
    private final int maxBackgroundCompactions;
    private final int statDumpSeconds;
    private final long syncMillis;
    private final long syncWarningNanos;
    private final Path storagePath;
    private final boolean adviseRandomOnOpen;
    private final boolean createIfMissing;
    private final boolean createMissingColumnFamilies;
    private final boolean useFsync;

    private final Set<byte[]> columnFamilyNames;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandles;

    private final boolean periodicSyncEnabled;
    private final ScheduledExecutorService syncExecutor;
    private final ReentrantLock syncLock = new ReentrantLock();
    private final Condition syncCondition = syncLock.newCondition();
    private final AtomicInteger syncCounter = new AtomicInteger(0);

    private volatile RocksDB rocksDB = null;
    private final ReentrantReadWriteLock dbReadWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock dbReadLock = dbReadWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock dbWriteLock = dbReadWriteLock.writeLock();
    private volatile boolean closed = false;

    private ColumnFamilyHandle configurationColumnFamilyHandle;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    private WriteOptions forceSyncWriteOptions;
    private WriteOptions noSyncWriteOptions;

    private RocksDBMetronome(Builder builder) {
        statDumpSeconds = builder.statDumpSeconds;
        parallelThreads = builder.parallelThreads;
        maxWriteBufferNumber = builder.maxWriteBufferNumber;
        minWriteBufferNumberToMerge = builder.minWriteBufferNumberToMerge;
        writeBufferSize = builder.writeBufferSize;
        maxTotalWalSize = builder.getMaxTotalWalSize();
        delayedWriteRate = builder.delayedWriteRate;
        level0SlowdownWritesTrigger = builder.level0SlowdownWritesTrigger;
        level0StopWritesTrigger = builder.level0StopWritesTrigger;
        maxBackgroundFlushes = builder.maxBackgroundFlushes;
        maxBackgroundCompactions = builder.maxBackgroundCompactions;
        syncMillis = builder.syncMillis;
        syncWarningNanos = builder.syncWarningNanos;
        storagePath = builder.storagePath;
        adviseRandomOnOpen = builder.adviseRandomOnOpen;
        createIfMissing = builder.createIfMissing;
        createMissingColumnFamilies = builder.createMissingColumnFamilies;
        useFsync = builder.useFsync;
        columnFamilyNames = builder.columnFamilyNames;
        columnFamilyHandles = new HashMap<>(columnFamilyNames.size());

        periodicSyncEnabled = builder.periodicSyncEnabled;
        syncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Initialize the metronome
     *
     * @throws IOException if there is an issue with the underlying database
     */
    public void initialize() throws IOException {

        final String rocksSharedLibDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
        final String javaTmpDir = System.getProperty("java.io.tmpdir");
        final String libDir = !StringUtils.isBlank(rocksSharedLibDir) ? rocksSharedLibDir : javaTmpDir;
        try {
            Files.createDirectories(Paths.get(libDir));
        } catch (IOException e) {
            throw new IOException("Unable to load the RocksDB shared library into directory: " + libDir, e);
        }

        // delete any previous librocksdbjni.so files
        final File[] rocksSos = Paths.get(libDir).toFile().listFiles((dir, name) -> name.startsWith("librocksdbjni") && name.endsWith(".so"));
        if (rocksSos != null) {
            for (File rocksSo : rocksSos) {
                if (!rocksSo.delete()) {
                    logger.warn("Could not delete existing librocksdbjni*.so file {}", rocksSo);
                }
            }
        }

        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            if (System.getProperty("os.name").startsWith("Windows")) {
                logger.error("The RocksDBMetronome will only work on Windows if you have Visual C++ runtime libraries for Visual Studio 2015 installed.  " +
                        "If the DLLs required to support RocksDB cannot be found, then NiFi will not start!");
            }
            throw t;
        }

        Files.createDirectories(storagePath);

        forceSyncWriteOptions = new WriteOptions()
                .setDisableWAL(false)
                .setSync(true);

        noSyncWriteOptions = new WriteOptions()
                .setDisableWAL(false)
                .setSync(false);


        dbWriteLock.lock();
        try (final DBOptions dbOptions = new DBOptions()
                .setAccessHintOnCompactionStart(AccessHint.SEQUENTIAL)
                .setAdviseRandomOnOpen(adviseRandomOnOpen)
                .setAllowMmapWrites(false) // required to be false for RocksDB.syncWal() to work
                .setCreateIfMissing(createIfMissing)
                .setCreateMissingColumnFamilies(createMissingColumnFamilies)
                .setDelayedWriteRate(delayedWriteRate)
                .setIncreaseParallelism(parallelThreads)
                .setLogger(getRocksLogger())
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setMaxBackgroundFlushes(maxBackgroundFlushes)
                .setMaxTotalWalSize(maxTotalWalSize)
                .setStatsDumpPeriodSec(statDumpSeconds)
                .setUseFsync(useFsync)
             ;

             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                     .setCompressionType(CompressionType.LZ4_COMPRESSION)
                     .setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger)
                     .setLevel0StopWritesTrigger(level0StopWritesTrigger)
                     .setMaxWriteBufferNumber(maxWriteBufferNumber)
                     .setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
                     .setWriteBufferSize(writeBufferSize)
        ) {

            // create family descriptors
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(columnFamilyNames.size());
            for (byte[] name : columnFamilyNames) {
                familyDescriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }

            List<ColumnFamilyHandle> columnFamilyList = new ArrayList<>(columnFamilyNames.size());

            rocksDB = RocksDB.open(dbOptions, storagePath.toString(), familyDescriptors, columnFamilyList);

            // create the map of names to handles
            columnFamilyHandles.put(DEFAULT_FAMILY, rocksDB.getDefaultColumnFamily());
            for (ColumnFamilyHandle cf : columnFamilyList) {
                columnFamilyHandles.put(new String(cf.getName(), StandardCharsets.UTF_8), cf);
            }

            // set specific special handles
            defaultColumnFamilyHandle = rocksDB.getDefaultColumnFamily();
            configurationColumnFamilyHandle = columnFamilyHandles.get(CONFIGURATION_FAMILY);

        } catch (RocksDBException e) {
            throw new IOException(e);
        } finally {
            dbWriteLock.unlock();
        }

        if (periodicSyncEnabled) {
            syncExecutor.scheduleWithFixedDelay(this::doSync, syncMillis, syncMillis, TimeUnit.MILLISECONDS);
        }

        logger.info("Initialized RocksDB Repository at {}", storagePath);
    }


    /**
     * This method checks the state of the database to ensure it is available for use.
     * <p>
     * NOTE: This *must* be called holding the dbReadLock
     *
     * @throws IllegalStateException if the database is closed or not yet initialized
     */
    private void checkDbState() throws IllegalStateException {
        if (rocksDB == null) {
            if (closed) {
                throw new IllegalStateException("RocksDBMetronome is closed");
            }
            throw new IllegalStateException("RocksDBMetronome has not been initialized");
        }

    }

    /**
     * Return an iterator over the specified column family. The iterator is initially invalid (caller must call one of the Seek methods on the iterator before using it).
     * <p>
     * Caller should close the iterator when it is no longer needed. The returned iterator should be closed before this db is closed.
     *
     * @param columnFamilyHandle specifies the column family for the iterator
     * @return an iterator over the specified column family
     */
    public RocksIterator getIterator(final ColumnFamilyHandle columnFamilyHandle) {
        dbReadLock.lock();
        try {
            checkDbState();
            return rocksDB.newIterator(columnFamilyHandle);
        } finally {
            dbReadLock.unlock();
        }
    }

    /**
     * Get the value for the provided key in the specified column family
     *
     * @param columnFamilyHandle the column family from which to get the value
     * @param key                the key of the value to retrieve
     * @return the value for the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public byte[] get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key) throws RocksDBException {
        dbReadLock.lock();
        try {
            checkDbState();
            return rocksDB.get(columnFamilyHandle, key);
        } finally {
            dbReadLock.unlock();
        }
    }

    /**
     * Put the key / value pair into the database in the specified column family
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param writeOptions       specification of options for write operations
     * @param key                the key to be inserted
     * @param value              the value to be associated with the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void put(final ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOptions, final byte[] key, final byte[] value) throws RocksDBException {
        dbReadLock.lock();
        try {
            checkDbState();
            rocksDB.put(columnFamilyHandle, writeOptions, key, value);
        } finally {
            dbReadLock.unlock();
        }
    }

    /**
     * Delete the key / value pair from the specified column family
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param writeOptions       specification of options for write operations
     * @param key                the key to be inserted
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void delete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final WriteOptions writeOptions) throws RocksDBException {
        dbReadLock.lock();
        try {
            checkDbState();
            rocksDB.delete(columnFamilyHandle, writeOptions, key);
        } finally {
            dbReadLock.unlock();
        }
    }

    /**
     * Flushes the WAL and syncs to disk
     *
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void forceSync() throws RocksDBException {
        dbReadLock.lock();
        try {
            checkDbState();
            rocksDB.syncWal();
        } finally {
            dbReadLock.unlock();
        }
    }

    /**
     * Get the handle for the specified column family
     *
     * @param familyName the name of the column family
     * @return the handle
     */
    public ColumnFamilyHandle getColumnFamilyHandle(String familyName) {
        return columnFamilyHandles.get(familyName);
    }

    /**
     * Put the key / value pair into the configuration column family and sync the wal
     *
     * @param key   the key to be inserted
     * @param value the value to be associated with the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void putConfiguration(final byte[] key, final byte[] value) throws RocksDBException {
        put(configurationColumnFamilyHandle, forceSyncWriteOptions, key, value);
    }

    /**
     * Put the key / value pair into the database in the specified column family without syncing the wal
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param key                the key to be inserted
     * @param value              the value to be associated with the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value) throws RocksDBException {
        put(columnFamilyHandle, noSyncWriteOptions, key, value);
    }

    /**
     * Put the key / value pair into the database in the specified column family, optionally syncing the wal
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param key                the key to be inserted
     * @param value              the value to be associated with the specified key
     * @param forceSync          if true, sync the wal
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value, final boolean forceSync) throws RocksDBException {
        put(columnFamilyHandle, getWriteOptions(forceSync), key, value);
    }

    /**
     * Put the key / value pair into the database in the default column family, optionally syncing the wal
     *
     * @param key       the key to be inserted
     * @param value     the value to be associated with the specified key
     * @param forceSync if true, sync the wal
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void put(final byte[] key, final byte[] value, final boolean forceSync) throws RocksDBException {
        put(defaultColumnFamilyHandle, getWriteOptions(forceSync), key, value);
    }

    /**
     * Put the key / value pair into the database in the default column family, without syncing the wal
     *
     * @param key   the key to be inserted
     * @param value the value to be associated with the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void put(final byte[] key, final byte[] value) throws RocksDBException {
        put(defaultColumnFamilyHandle, noSyncWriteOptions, key, value);
    }

    /**
     * Get the value for the provided key in the default column family
     *
     * @param key the key of the value to retrieve
     * @return the value for the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public byte[] get(final byte[] key) throws RocksDBException {
        return get(defaultColumnFamilyHandle, key);
    }

    /**
     * Get the value for the provided key in the configuration column family
     *
     * @param key the key of the value to retrieve
     * @return the value for the specified key
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public byte[] getConfiguration(final byte[] key) throws RocksDBException {
        return get(configurationColumnFamilyHandle, key);
    }


    /**
     * Delete the key / value pair from the default column family without syncing the wal
     *
     * @param key the key to be inserted
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void delete(byte[] key) throws RocksDBException {
        delete(defaultColumnFamilyHandle, key, noSyncWriteOptions);
    }

    /**
     * Delete the key / value pair from the default column family, optionally syncing the wal
     *
     * @param key       the key to be inserted
     * @param forceSync if true, sync the wal
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void delete(byte[] key, final boolean forceSync) throws RocksDBException {
        delete(defaultColumnFamilyHandle, key, getWriteOptions(forceSync));
    }

    /**
     * Delete the key / value pair from the default column family without syncing the wal
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param key                the key to be inserted
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void delete(final ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        delete(columnFamilyHandle, key, noSyncWriteOptions);
    }

    /**
     * Delete the key / value pair from the specified column family, optionally syncing the wal
     *
     * @param columnFamilyHandle the column family in to which to put the value
     * @param key                the key to be inserted
     * @param forceSync          if true, sync the wal
     * @throws RocksDBException thrown if there is an error in the underlying library.
     */
    public void delete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final boolean forceSync) throws RocksDBException {
        delete(columnFamilyHandle, key, getWriteOptions(forceSync));
    }

    private WriteOptions getWriteOptions(boolean forceSync) {
        return forceSync ? forceSyncWriteOptions : noSyncWriteOptions;
    }

    /**
     * Return an iterator over the default column family. The iterator is initially invalid (caller must call one of the Seek methods on the iterator before using it).
     * <p>
     * Caller should close the iterator when it is no longer needed. The returned iterator should be closed before this db is closed.
     *
     * @return an iterator over the default column family
     */
    public RocksIterator getIterator() {
        return getIterator(defaultColumnFamilyHandle);
    }

    /**
     * Get the current value of the sync counter.  This can be used with waitForSync() to verify that operations have been synced to disk
     *
     * @return the current value of the sync counter
     */
    public int getSyncCounterValue() {
        return syncCounter.get();
    }

    /**
     * Close the Metronome and the RocksDB Objects
     *
     * @throws IOException if there are issues in closing any of the underlying RocksDB objects
     */
    @Override
    public void close() throws IOException {

        logger.info("Closing RocksDBMetronome");

        dbWriteLock.lock();
        try {
            logger.info("Shutting down RocksDBMetronome sync executor");
            syncExecutor.shutdownNow();

            try {
                logger.info("Pausing RocksDB background work");
                rocksDB.pauseBackgroundWork();
            } catch (RocksDBException e) {
                logger.warn("Unable to pause background work before close.", e);
            }

            final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

            logger.info("Closing RocksDB configurations");

            safeClose(forceSyncWriteOptions, exceptionReference);
            safeClose(noSyncWriteOptions, exceptionReference);

            // close the column family handles first, then the db last
            for (ColumnFamilyHandle cfh : columnFamilyHandles.values()) {
                safeClose(cfh, exceptionReference);
            }

            logger.info("Closing RocksDB database");
            safeClose(rocksDB, exceptionReference);
            rocksDB = null;
            closed = true;

            if (exceptionReference.get() != null) {
                throw new IOException(exceptionReference.get());
            }
        } finally {
            dbWriteLock.unlock();
        }
    }

    /**
     * @param autoCloseable      An {@link AutoCloseable} to be closed
     * @param exceptionReference A reference to contain any encountered {@link Exception}
     */
    private void safeClose(final AutoCloseable autoCloseable, final AtomicReference<Exception> exceptionReference) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                exceptionReference.set(e);
            }
        }
    }

    /**
     * @return The capacity of the store
     * @throws IOException if encountered
     */
    public long getStorageCapacity() throws IOException {
        return Files.getFileStore(storagePath).getTotalSpace();
    }

    /**
     * @return The usable space of the store
     * @throws IOException if encountered
     */
    public long getUsableStorageSpace() throws IOException {
        return Files.getFileStore(storagePath).getUsableSpace();
    }

    /**
     * This method is scheduled by the syncExecutor.  It runs at the specified interval, forcing the RocksDB WAL to disk.
     * <p>
     * If the sync is successful, it notifies threads that had been waiting for their records to be persisted using
     * syncCondition and the syncCounter, which is incremented to indicate success
     */
    void doSync() {
        syncLock.lock();
        try {
            // if we're interrupted, return
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            forceSync();
            syncCounter.incrementAndGet(); // its ok if it rolls over... we're just going to check that the value changed
            syncCondition.signalAll();
        } catch (final IllegalArgumentException e) {
            logger.error("Unable to sync, likely because the repository is out of space.", e);
        } catch (final Throwable t) {
            logger.error("Unable to sync", t);
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * This method blocks until the next time the WAL is forced to disk, ensuring that all records written before this point have been persisted.
     */
    public void waitForSync() throws InterruptedException {
        final int counterValue = syncCounter.get();
        waitForSync(counterValue);
    }

    /**
     * This method blocks until the WAL has been forced to disk, ensuring that all records written before the point specified by the counterValue have been persisted.
     *
     * @param counterValue The value of the counter at the time of a write we must persist
     * @throws InterruptedException if the thread is interrupted
     */
    public void waitForSync(final int counterValue) throws InterruptedException {
        if (counterValue != syncCounter.get()) {
            return; // if the counter has already changed, we don't need to wait (or grab the lock) because the records we're concerned with have already been persisted
        }
        long waitTimeRemaining = syncWarningNanos;
        syncLock.lock();
        try {
            while (counterValue == syncCounter.get()) {  // wait until the counter changes (indicating sync occurred)
                waitTimeRemaining = syncCondition.awaitNanos(waitTimeRemaining);
                if (waitTimeRemaining <= 0L) { // this means the wait timed out

                    // only log a warning every syncWarningNanos... don't spam the logs
                    final long now = System.nanoTime();
                    final long lastWarning = lastSyncWarningNanos.get();
                    if (now - lastWarning > syncWarningNanos
                            && lastSyncWarningNanos.compareAndSet(lastWarning, now)) {
                        logger.warn("Failed to sync within {} seconds... system configuration may need to be adjusted", TimeUnit.NANOSECONDS.toSeconds(syncWarningNanos));
                    }

                    // reset waiting time
                    waitTimeRemaining = syncWarningNanos;
                }
            }
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * Returns a representation of a long as a byte array
     *
     * @param value a long to convert
     * @return a byte[] representation
     */
    public static byte[] getBytes(long value) {
        byte[] bytes = new byte[8];
        writeLong(value, bytes);
        return bytes;
    }

    /**
     * Writes a representation of a long to the specified byte array
     *
     * @param l     a long to convert
     * @param bytes an array to store the byte representation
     */
    public static void writeLong(long l, byte[] bytes) {
        bytes[0] = (byte) (l >>> 56);
        bytes[1] = (byte) (l >>> 48);
        bytes[2] = (byte) (l >>> 40);
        bytes[3] = (byte) (l >>> 32);
        bytes[4] = (byte) (l >>> 24);
        bytes[5] = (byte) (l >>> 16);
        bytes[6] = (byte) (l >>> 8);
        bytes[7] = (byte) (l);
    }

    /**
     * Creates a long from it's byte array representation
     * @param bytes to convert to a long
     * @return a long
     * @throws IOException if the given byte array is of the wrong size
     */
    public static long readLong(final byte[] bytes) throws IOException {
        if (bytes.length != 8) {
            throw new IOException("wrong number of bytes to convert to long (must be 8)");
        }
        return (((long) (bytes[0]) << 56) +
                ((long) (bytes[1] & 255) << 48) +
                ((long) (bytes[2] & 255) << 40) +
                ((long) (bytes[3] & 255) << 32) +
                ((long) (bytes[4] & 255) << 24) +
                ((long) (bytes[5] & 255) << 16) +
                ((long) (bytes[6] & 255) << 8) +
                ((long) (bytes[7] & 255)));
    }

    /**
     * @return the storage path of the db
     */
    public Path getStoragePath() {
        return storagePath;
    }

    /**
     * @return A RocksDB logger capturing all logging output from RocksDB
     */
    private org.rocksdb.Logger getRocksLogger() {
        try (Options options = new Options()
                // make RocksDB give us everything, and we'll decide what we want to log in our wrapper
                .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
            return new LogWrapper(options);
        }
    }

    /**
     * An Extension of org.rocksdb.Logger that wraps the slf4j Logger
     */
    private class LogWrapper extends org.rocksdb.Logger {

        LogWrapper(Options options) {
            super(options);
        }

        @Override
        protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            switch (infoLogLevel) {
                case ERROR_LEVEL:
                case FATAL_LEVEL:
                    logger.error(logMsg);
                    break;
                case WARN_LEVEL:
                    logger.warn(logMsg);
                    break;
                case DEBUG_LEVEL:
                    logger.debug(logMsg);
                    break;
                case INFO_LEVEL:
                case HEADER_LEVEL:
                default:
                    logger.info(logMsg);
                    break;
            }
        }
    }

    public static class Builder {

        int parallelThreads = 8;
        int maxWriteBufferNumber = 4;
        int minWriteBufferNumberToMerge = 1;
        long writeBufferSize = (long) DataUnit.MB.toB(256);
        long delayedWriteRate = (long) DataUnit.MB.toB(16);
        int level0SlowdownWritesTrigger = 20;
        int level0StopWritesTrigger = 40;
        int maxBackgroundFlushes = 1;
        int maxBackgroundCompactions = 1;
        int statDumpSeconds = 600;
        long syncMillis = 10;
        long syncWarningNanos = TimeUnit.SECONDS.toNanos(30);
        Path storagePath;
        boolean adviseRandomOnOpen = false;
        boolean createIfMissing = true;
        boolean createMissingColumnFamilies = true;
        boolean useFsync = true;
        boolean periodicSyncEnabled = true;
        final Set<byte[]> columnFamilyNames = new HashSet<>();

        public RocksDBMetronome build() {
            if (storagePath == null) {
                throw new IllegalStateException("Cannot create RocksDBMetronome because storagePath is not set");
            }

            // add default column families
            columnFamilyNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            columnFamilyNames.add(CONFIGURATION_FAMILY.getBytes(StandardCharsets.UTF_8));

            return new RocksDBMetronome(this);
        }

        public Builder addColumnFamily(String name) {
            this.columnFamilyNames.add(name.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        public Builder setStoragePath(Path storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder setParallelThreads(int parallelThreads) {
            this.parallelThreads = parallelThreads;
            return this;
        }

        public Builder setMaxWriteBufferNumber(int maxWriteBufferNumber) {
            this.maxWriteBufferNumber = maxWriteBufferNumber;
            return this;
        }

        public Builder setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge) {
            this.minWriteBufferNumberToMerge = minWriteBufferNumberToMerge;
            return this;
        }

        public Builder setWriteBufferSize(long writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder setDelayedWriteRate(long delayedWriteRate) {
            this.delayedWriteRate = delayedWriteRate;
            return this;
        }

        public Builder setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
            this.level0SlowdownWritesTrigger = level0SlowdownWritesTrigger;
            return this;
        }

        public Builder setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
            this.level0StopWritesTrigger = level0StopWritesTrigger;
            return this;
        }

        public Builder setMaxBackgroundFlushes(int maxBackgroundFlushes) {
            this.maxBackgroundFlushes = maxBackgroundFlushes;
            return this;
        }

        public Builder setMaxBackgroundCompactions(int maxBackgroundCompactions) {
            this.maxBackgroundCompactions = maxBackgroundCompactions;
            return this;
        }

        public Builder setStatDumpSeconds(int statDumpSeconds) {
            this.statDumpSeconds = statDumpSeconds;
            return this;
        }

        public Builder setSyncMillis(long syncMillis) {
            this.syncMillis = syncMillis;
            return this;
        }

        public Builder setSyncWarningNanos(long syncWarningNanos) {
            this.syncWarningNanos = syncWarningNanos;
            return this;
        }


        public Builder setAdviseRandomOnOpen(boolean adviseRandomOnOpen) {
            this.adviseRandomOnOpen = adviseRandomOnOpen;
            return this;
        }

        public Builder setCreateMissingColumnFamilies(boolean createMissingColumnFamilies) {
            this.createMissingColumnFamilies = createMissingColumnFamilies;
            return this;
        }

        public Builder setCreateIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        public Builder setUseFsync(boolean useFsync) {
            this.useFsync = useFsync;
            return this;
        }

        public Builder setPeriodicSyncEnabled(boolean periodicSyncEnabled) {
            this.periodicSyncEnabled = periodicSyncEnabled;
            return this;
        }

        long getMaxTotalWalSize() {
            return writeBufferSize * maxWriteBufferNumber;
        }
    }
}
