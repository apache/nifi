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

package org.apache.nifi.wali;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;
import org.wali.SerDeFactory;
import org.wali.UpdateType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HashMapSnapshot<T> implements WriteAheadSnapshot<T>, RecordLookup<T> {
    private static final Logger logger = LoggerFactory.getLogger(HashMapSnapshot.class);
    private static final int ENCODING_VERSION = 1;

    private final ConcurrentMap<Object, T> recordMap = new ConcurrentHashMap<>();
    private final SerDeFactory<T> serdeFactory;
    private final Set<String> swapLocations = Collections.synchronizedSet(new HashSet<>());
    private final File storageDirectory;

    public HashMapSnapshot(final File storageDirectory, final SerDeFactory<T> serdeFactory) {
        this.serdeFactory = serdeFactory;
        this.storageDirectory = storageDirectory;
    }

    private SnapshotHeader validateHeader(final DataInputStream dataIn) throws IOException {
        final String snapshotClass = dataIn.readUTF();
        logger.debug("Snapshot Class Name for {} is {}", storageDirectory, snapshotClass);
        if (!snapshotClass.equals(HashMapSnapshot.class.getName())) {
            throw new IOException("Write-Ahead Log Snapshot located at " + storageDirectory + " was written using the "
                + snapshotClass + " class; cannot restore using " + getClass().getName());
        }

        final int snapshotVersion = dataIn.readInt();
        logger.debug("Snapshot version for {} is {}", storageDirectory, snapshotVersion);
        if (snapshotVersion > getVersion()) {
            throw new IOException("Write-Ahead Log Snapshot located at " + storageDirectory + " was written using version "
                + snapshotVersion + " of the " + snapshotClass + " class; cannot restore using Version " + getVersion());
        }

        final String serdeEncoding = dataIn.readUTF(); // ignore serde class name for now
        logger.debug("Serde encoding for Snapshot at {} is {}", storageDirectory, serdeEncoding);

        final int serdeVersion = dataIn.readInt();
        logger.debug("Serde version for Snapshot at {} is {}", storageDirectory, serdeVersion);

        final long maxTransactionId = dataIn.readLong();
        logger.debug("Max Transaction ID for Snapshot at {} is {}", storageDirectory, maxTransactionId);

        final int numRecords = dataIn.readInt();
        logger.debug("Number of Records for Snapshot at {} is {}", storageDirectory, numRecords);

        final SerDe<T> serde = serdeFactory.createSerDe(serdeEncoding);
        serde.readHeader(dataIn);

        return new SnapshotHeader(serde, serdeVersion, maxTransactionId, numRecords);
    }

    @Override
    public SnapshotRecovery<T> recover() throws IOException {
        final File partialFile = getPartialFile();
        final File snapshotFile = getSnapshotFile();
        final boolean partialExists = partialFile.exists();
        final boolean snapshotExists = snapshotFile.exists();

        // If there is no snapshot (which is the case before the first snapshot is ever created), then just
        // return an empty recovery.
        if (!partialExists && !snapshotExists) {
            return SnapshotRecovery.emptyRecovery();
        }

        if (partialExists && snapshotExists) {
            // both files exist -- assume NiFi crashed/died while checkpointing. Delete the partial file.
            Files.delete(partialFile.toPath());
        } else if (partialExists) {
            // partial exists but snapshot does not -- we must have completed
            // creating the partial, deleted the snapshot
            // but crashed before renaming the partial to the snapshot. Just
            // rename partial to snapshot
            Files.move(partialFile.toPath(), snapshotFile.toPath());
        }

        if (snapshotFile.length() == 0) {
            logger.warn("{} Found 0-byte Snapshot file; skipping Snapshot file in recovery", this);
            return SnapshotRecovery.emptyRecovery();
        }

        // At this point, we know the snapshotPath exists because if it didn't, then we either returned null
        // or we renamed partialPath to snapshotPath. So just Recover from snapshotPath.
        try (final DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile)))) {
            // Ensure that the header contains the information that we expect and retrieve the relevant information from the header.
            final SnapshotHeader header = validateHeader(dataIn);

            final SerDe<T> serde = header.getSerDe();
            final int serdeVersion = header.getSerDeVersion();
            final int numRecords = header.getNumRecords();
            final long maxTransactionId = header.getMaxTransactionId();

            // Read all of the records that we expect to receive.
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

            // Determine the location of any swap files.
            final int numSwapRecords = dataIn.readInt();
            final Set<String> swapLocations = new HashSet<>();
            for (int i = 0; i < numSwapRecords; i++) {
                swapLocations.add(dataIn.readUTF());
            }
            this.swapLocations.addAll(swapLocations);

            logger.info("{} restored {} Records and {} Swap Files from Snapshot, ending with Transaction ID {}",
                new Object[] {this, numRecords, swapLocations.size(), maxTransactionId});

            return new StandardSnapshotRecovery<>(recordMap, swapLocations, snapshotFile, maxTransactionId);
        }
    }

    @Override
    public void update(final Collection<T> records) {
        // This implementation of Snapshot keeps a ConcurrentHashMap of all 'active' records
        // (meaning records that have not been removed and are not swapped out), keyed by the
        // Record Identifier. It keeps only the most up-to-date version of the Record. This allows
        // us to write the snapshot very quickly without having to re-process the journal files.
        // For each update, then, we will update the record in the map.
        for (final T record : records) {
            final Object recordId = serdeFactory.getRecordIdentifier(record);
            final UpdateType updateType = serdeFactory.getUpdateType(record);

            switch (updateType) {
                case DELETE:
                    recordMap.remove(recordId);
                    break;
                case SWAP_OUT:
                    final String location = serdeFactory.getLocation(record);
                    if (location == null) {
                        logger.error("Received Record (ID=" + recordId + ") with UpdateType of SWAP_OUT but "
                            + "no indicator of where the Record is to be Swapped Out to; these records may be "
                            + "lost when the repository is restored!");
                    } else {
                        recordMap.remove(recordId);
                        this.swapLocations.add(location);
                    }
                    break;
                case SWAP_IN:
                    final String swapLocation = serdeFactory.getLocation(record);
                    if (swapLocation == null) {
                        logger.error("Received Record (ID=" + recordId + ") with UpdateType of SWAP_IN but no "
                            + "indicator of where the Record is to be Swapped In from; these records may be duplicated "
                            + "when the repository is restored!");
                    } else {
                        swapLocations.remove(swapLocation);
                    }
                    recordMap.put(recordId, record);
                    break;
                default:
                    recordMap.put(recordId, record);
                    break;
            }
        }
    }

    @Override
    public int getRecordCount() {
        return recordMap.size();
    }

    @Override
    public T lookup(final Object recordId) {
        return recordMap.get(recordId);
    }

    @Override
    public SnapshotCapture<T> prepareSnapshot(final long maxTransactionId) {
        return prepareSnapshot(maxTransactionId, this.swapLocations);
    }

    @Override
    public SnapshotCapture<T> prepareSnapshot(final long maxTransactionId, final Set<String> swapFileLocations) {
        return new Snapshot(new HashMap<>(recordMap), new HashSet<>(swapFileLocations), maxTransactionId);
    }

    private int getVersion() {
        return ENCODING_VERSION;
    }

    private File getPartialFile() {
        return new File(storageDirectory, "checkpoint.partial");
    }

    private File getSnapshotFile() {
        return new File(storageDirectory, "checkpoint");
    }

    @Override
    public synchronized void writeSnapshot(final SnapshotCapture<T> snapshot) throws IOException {
        final SerDe<T> serde = serdeFactory.createSerDe(null);

        final File snapshotFile = getSnapshotFile();
        final File partialFile = getPartialFile();

        // We must ensure that we do not overwrite the existing Snapshot file directly because if NiFi were
        // to be killed or crash when we are partially finished, we'd end up with no viable Snapshot file at all.
        // To avoid this, we write to a 'partial' file, then delete the existing Snapshot file, if it exists, and
        // rename 'partial' to Snaphsot. That way, if NiFi crashes, we can still restore the Snapshot by first looking
        // for a Snapshot file and restoring it, if it exists. If it does not exist, then we restore from the partial file,
        // assuming that NiFi crashed after deleting the Snapshot file and before renaming the partial file.
        //
        // If there is no Snapshot file currently but there is a Partial File, then this indicates
        // that we have deleted the Snapshot file and failed to rename the Partial File. We don't want
        // to overwrite the Partial file, because doing so could potentially lose data. Instead, we must
        // first rename it to Snapshot and then write to the partial file.
        if (!snapshotFile.exists() && partialFile.exists()) {
            final boolean rename = partialFile.renameTo(snapshotFile);
            if (!rename) {
                throw new IOException("Failed to rename partial snapshot file " + partialFile + " to " + snapshotFile);
            }
        }

        // Write to the partial file.
        try (final FileOutputStream fileOut = new FileOutputStream(getPartialFile());
            final OutputStream bufferedOut = new BufferedOutputStream(fileOut);
            final DataOutputStream dataOut = new DataOutputStream(bufferedOut)) {

            // Write out the header
            dataOut.writeUTF(HashMapSnapshot.class.getName());
            dataOut.writeInt(getVersion());
            dataOut.writeUTF(serde.getClass().getName());
            dataOut.writeInt(serde.getVersion());
            dataOut.writeLong(snapshot.getMaxTransactionId());
            dataOut.writeInt(snapshot.getRecords().size());
            serde.writeHeader(dataOut);

            // Serialize each record
            for (final T record : snapshot.getRecords().values()) {
                logger.trace("Checkpointing {}", record);
                serde.serializeRecord(record, dataOut);
            }

            // Write out the number of swap locations, followed by the swap locations themselves.
            dataOut.writeInt(snapshot.getSwapLocations().size());
            for (final String swapLocation : snapshot.getSwapLocations()) {
                dataOut.writeUTF(swapLocation);
            }

            // Ensure that we flush the Buffered Output Stream and then perform an fsync().
            // This ensures that the data is fully written to disk before we delete the existing snapshot.
            dataOut.flush();
            fileOut.getChannel().force(false);
        }

        // If the snapshot file exists, delete it
        if (snapshotFile.exists()) {
            if (!snapshotFile.delete()) {
                logger.warn("Unable to delete existing Snapshot file " + snapshotFile);
            }
        }

        // Rename the partial file to Snapshot.
        final boolean rename = partialFile.renameTo(snapshotFile);
        if (!rename) {
            throw new IOException("Failed to rename partial snapshot file " + partialFile + " to " + snapshotFile);
        }
    }


    public class Snapshot implements SnapshotCapture<T> {
        private final Map<Object, T> records;
        private final long maxTransactionId;
        private final Set<String> swapLocations;

        public Snapshot(final Map<Object, T> records, final Set<String> swapLocations, final long maxTransactionId) {
            this.records = records;
            this.swapLocations = swapLocations;
            this.maxTransactionId = maxTransactionId;
        }

        @Override
        public final Map<Object, T> getRecords() {
            return records;
        }

        @Override
        public long getMaxTransactionId() {
            return maxTransactionId;
        }

        @Override
        public Set<String> getSwapLocations() {
            return swapLocations;
        }
    }

    private class SnapshotHeader {
        private final SerDe<T> serde;
        private final int serdeVersion;
        private final int numRecords;
        private final long maxTransactionId;

        public SnapshotHeader(final SerDe<T> serde, final int serdeVersion, final long maxTransactionId, final int numRecords) {
            this.serde = serde;
            this.serdeVersion = serdeVersion;
            this.maxTransactionId = maxTransactionId;
            this.numRecords = numRecords;
        }

        public SerDe<T> getSerDe() {
            return serde;
        }

        public int getSerDeVersion() {
            return serdeVersion;
        }

        public long getMaxTransactionId() {
            return maxTransactionId;
        }

        public int getNumRecords() {
            return numRecords;
        }
    }

}
