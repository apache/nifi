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

import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;
import org.wali.SerDeFactory;
import org.wali.UpdateType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LengthDelimitedJournal<T> implements WriteAheadJournal<T> {
    private static final Logger logger = LoggerFactory.getLogger(LengthDelimitedJournal.class);
    private static final int DEFAULT_MAX_IN_HEAP_SERIALIZATION_BYTES = 5 * 1024 * 1024; // 5 MB

    private static final JournalSummary INACTIVE_JOURNAL_SUMMARY = new StandardJournalSummary(-1L, -1L, 0);
    private static final int JOURNAL_ENCODING_VERSION = 1;
    private static final byte TRANSACTION_FOLLOWS = 64;
    private static final byte JOURNAL_COMPLETE = 127;
    private static final int NUL_BYTE = 0;

    private final File journalFile;
    private final File overflowDirectory;
    private final long initialTransactionId;
    private final SerDeFactory<T> serdeFactory;
    private final ObjectPool<ByteArrayDataOutputStream> streamPool;
    private final int maxInHeapSerializationBytes;

    private SerDe<T> serde;
    private FileOutputStream fileOut;
    private BufferedOutputStream bufferedOut;

    private long currentTransactionId;
    private int transactionCount;
    private boolean headerWritten = false;

    private volatile boolean poisoned = false;
    private volatile boolean closed = false;
    private final ByteBuffer transactionPreamble = ByteBuffer.allocate(12); // guarded by synchronized block

    public LengthDelimitedJournal(final File journalFile, final SerDeFactory<T> serdeFactory, final ObjectPool<ByteArrayDataOutputStream> streamPool, final long initialTransactionId) {
        this(journalFile, serdeFactory, streamPool, initialTransactionId, DEFAULT_MAX_IN_HEAP_SERIALIZATION_BYTES);
    }

    public LengthDelimitedJournal(final File journalFile, final SerDeFactory<T> serdeFactory, final ObjectPool<ByteArrayDataOutputStream> streamPool, final long initialTransactionId,
                                  final int maxInHeapSerializationBytes) {
        this.journalFile = journalFile;
        this.overflowDirectory = new File(journalFile.getParentFile(), "overflow-" + getBaseFilename(journalFile));
        this.serdeFactory = serdeFactory;
        this.serde = serdeFactory.createSerDe(null);
        this.streamPool = streamPool;

        this.initialTransactionId = initialTransactionId;
        this.currentTransactionId = initialTransactionId;
        this.maxInHeapSerializationBytes = maxInHeapSerializationBytes;
    }

    public void dispose() {
        logger.debug("Deleting Journal {} because it is now encapsulated in the latest Snapshot", journalFile.getName());
        if (!journalFile.delete() && journalFile.exists()) {
            logger.warn("Unable to delete expired journal file " + journalFile + "; this file should be deleted manually.");
        }

        if (overflowDirectory.exists()) {
            final File[] overflowFiles = overflowDirectory.listFiles();
            if (overflowFiles == null) {
                logger.warn("Unable to obtain listing of files that exist in 'overflow directory' " + overflowDirectory
                    + " - this directory and any files within it can now be safely removed manually");
                return;
            }

            for (final File overflowFile : overflowFiles) {
                if (!overflowFile.delete() && overflowFile.exists()) {
                    logger.warn("After expiring journal file " + journalFile + ", unable to remove 'overflow file' " + overflowFile + " - this file should be removed manually");
                }
            }

            if (!overflowDirectory.delete()) {
                logger.warn("After expiring journal file " + journalFile + ", unable to remove 'overflow directory' " + overflowDirectory + " - this file should be removed manually");
            }
        }
    }

    private static String getBaseFilename(final File file) {
        final String name = file.getName();
        final int index = name.lastIndexOf(".");
        if (index < 0) {
            return name;
        }

        return name.substring(0, index);
    }

    private synchronized OutputStream getOutputStream() throws FileNotFoundException {
        if (fileOut == null) {
            fileOut = new FileOutputStream(journalFile);
            bufferedOut = new BufferedOutputStream(fileOut);
        }

        return bufferedOut;
    }

    @Override
    public synchronized boolean isHealthy() {
        return !closed && !poisoned;
    }

    @Override
    public synchronized void writeHeader() throws IOException {
        try {
            final DataOutputStream outStream = new DataOutputStream(getOutputStream());
            outStream.writeUTF(LengthDelimitedJournal.class.getName());
            outStream.writeInt(JOURNAL_ENCODING_VERSION);

            serde = serdeFactory.createSerDe(null);
            outStream.writeUTF(serde.getClass().getName());
            outStream.writeInt(serde.getVersion());

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(baos)) {

                serde.writeHeader(dos);
                dos.flush();

                final int serdeHeaderLength = baos.size();
                outStream.writeInt(serdeHeaderLength);
                baos.writeTo(outStream);
            }

            outStream.flush();
        } catch (final Throwable t) {
            poison(t);

            final IOException ioe = (t instanceof IOException) ? (IOException) t : new IOException("Failed to create journal file " + journalFile, t);
            logger.error("Failed to create new journal file {} due to {}", journalFile, ioe.toString(), ioe);
            throw ioe;
        }

        headerWritten = true;
    }

    private synchronized SerDeAndVersion validateHeader(final DataInputStream in) throws IOException {
        final String journalClassName = in.readUTF();
        logger.debug("Write Ahead Log Class Name for {} is {}", journalFile, journalClassName);
        if (!LengthDelimitedJournal.class.getName().equals(journalClassName)) {
            throw new IOException("Invalid header information - " + journalFile + " does not appear to be a valid journal file.");
        }

        final int encodingVersion = in.readInt();
        logger.debug("Encoding version for {} is {}", journalFile, encodingVersion);
        if (encodingVersion > JOURNAL_ENCODING_VERSION) {
            throw new IOException("Cannot read journal file " + journalFile + " because it is encoded using veresion " + encodingVersion
                + " but this version of the code only understands version " + JOURNAL_ENCODING_VERSION + " and below");
        }

        final String serdeClassName = in.readUTF();
        logger.debug("Serde Class Name for {} is {}", journalFile, serdeClassName);
        final SerDe<T> serde;
        try {
            serde = serdeFactory.createSerDe(serdeClassName);
        } catch (final IllegalArgumentException iae) {
            throw new IOException("Cannot read journal file " + journalFile + " because the serializer/deserializer used was " + serdeClassName
                + " but this repository is configured to use a different type of serializer/deserializer");
        }

        final int serdeVersion = in.readInt();
        logger.debug("Serde version is {}", serdeVersion);
        if (serdeVersion > serde.getVersion()) {
            throw new IOException("Cannot read journal file " + journalFile + " because it is encoded using veresion " + encodingVersion
                + " of the serializer/deserializer but this version of the code only understands version " + serde.getVersion() + " and below");
        }

        final int serdeHeaderLength = in.readInt();
        final InputStream serdeHeaderIn = new LimitingInputStream(in, serdeHeaderLength);
        final DataInputStream dis = new DataInputStream(serdeHeaderIn);
        serde.readHeader(dis);

        return new SerDeAndVersion(serde, serdeVersion);
    }


    @Override
    public void update(final Collection<T> records, final RecordLookup<T> recordLookup) throws IOException {
        if (!headerWritten) {
            throw new IllegalStateException("Cannot update journal file " + journalFile + " because no header has been written yet.");
        }

        if (records.isEmpty()) {
            return;
        }

        checkState();

        File overflowFile = null;
        final ByteArrayDataOutputStream bados = streamPool.borrowObject();

        try {
            FileOutputStream overflowFileOut = null;

            try {
                DataOutputStream dataOut = bados.getDataOutputStream();
                for (final T record : records) {
                    final Object recordId = serde.getRecordIdentifier(record);
                    final T previousRecordState = recordLookup.lookup(recordId);
                    serde.serializeEdit(previousRecordState, record, dataOut);

                    final int size = bados.getByteArrayOutputStream().size();
                    if (serde.isWriteExternalFileReferenceSupported() && size > maxInHeapSerializationBytes) {
                        if (!overflowDirectory.exists()) {
                            Files.createDirectory(overflowDirectory.toPath());
                        }

                        // If we have exceeded our threshold for how much to serialize in memory,
                        // flush the in-memory representation to an 'overflow file' and then update
                        // the Data Output Stream that is used to write to the file also.
                        overflowFile = new File(overflowDirectory, UUID.randomUUID().toString());
                        logger.debug("Length of update with {} records exceeds in-memory max of {} bytes. Overflowing to {}", records.size(), maxInHeapSerializationBytes, overflowFile);

                        overflowFileOut = new FileOutputStream(overflowFile);
                        bados.getByteArrayOutputStream().writeTo(overflowFileOut);
                        bados.getByteArrayOutputStream().reset();

                        // change dataOut to point to the File's Output Stream so that all subsequent records are written to the file.
                        dataOut = new DataOutputStream(new BufferedOutputStream(overflowFileOut));

                        // We now need to write to the ByteArrayOutputStream a pointer to the overflow file
                        // so that what is written to the actual journal is that pointer.
                        serde.writeExternalFileReference(overflowFile, bados.getDataOutputStream());
                    }
                }

                dataOut.flush();

                // If we overflowed to an external file, we need to be sure that we sync to disk before
                // updating the Journal. Otherwise, we could get to a state where the Journal was flushed to disk without the
                // external file being flushed. This would result in a missed update to the FlowFile Repository.
                if (overflowFileOut != null) {
                    if (logger.isDebugEnabled()) { // avoid calling File.length() if not necessary
                        logger.debug("Length of update to overflow file is {} bytes", overflowFile.length());
                    }

                    overflowFileOut.getFD().sync();
                }
            } finally {
                if (overflowFileOut != null) {
                    try {
                        overflowFileOut.close();
                    } catch (final Exception e) {
                        logger.warn("Failed to close open file handle to overflow file {}", overflowFile, e);
                    }
                }
            }

            final ByteArrayOutputStream baos = bados.getByteArrayOutputStream();
            final OutputStream out = getOutputStream();

            final long transactionId;
            synchronized (this) {
                transactionId = currentTransactionId++;
                transactionCount++;

                transactionPreamble.clear();
                transactionPreamble.putLong(transactionId);
                transactionPreamble.putInt(baos.size());

                out.write(TRANSACTION_FOLLOWS);
                out.write(transactionPreamble.array());
                baos.writeTo(out);
                out.flush();
            }

            logger.debug("Wrote Transaction {} to journal {} with length {} and {} records", transactionId, journalFile, baos.size(), records.size());
        } catch (final Throwable t) {
            poison(t);

            if (overflowFile != null) {
                if (!overflowFile.delete() && overflowFile.exists()) {
                    logger.warn("Failed to cleanup temporary overflow file " + overflowFile + " - this file should be cleaned up manually.");
                }
            }

            throw t;
        } finally {
            streamPool.returnObject(bados);
        }
    }


    private void checkState() throws IOException {
        if (poisoned) {
            throw new IOException("Cannot update journal file " + journalFile + " because this journal has already encountered a failure when attempting to write to the file. "
                + "If the repository is able to checkpoint, then this problem will resolve itself. However, if the repository is unable to be checkpointed "
                + "(for example, due to being out of storage space or having too many open files), then this issue may require manual intervention.");
        }

        if (closed) {
            throw new IOException("Cannot update journal file " + journalFile + " because this journal has already been closed");
        }
    }

    private void poison(final Throwable t) {
        this.poisoned = true;

        try {
            if (fileOut != null) {
                fileOut.close();
            }

            closed = true;
        } catch (final IOException innerIOE) {
            t.addSuppressed(innerIOE);
        }
    }

    @Override
    public synchronized void fsync() throws IOException {
        checkState();

        try {
            if (fileOut != null) {
                fileOut.getChannel().force(false);
            }
        } catch (final IOException ioe) {
            poison(ioe);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        try {
            if (fileOut != null) {
                if (!poisoned) {
                    fileOut.write(JOURNAL_COMPLETE);
                }

                fileOut.close();
            }
        } catch (final IOException ioe) {
            poison(ioe);
        }
    }

    @Override
    public JournalRecovery recoverRecords(final Map<Object, T> recordMap, final Set<String> swapLocations) throws IOException {
        long maxTransactionId = -1L;
        int updateCount = 0;

        boolean eofException = false;
        logger.info("Recovering records from journal {}", journalFile);
        final double journalLength = journalFile.length();

        try (final InputStream fis = new FileInputStream(journalFile);
            final InputStream bufferedIn = new BufferedInputStream(fis);
            final ByteCountingInputStream byteCountingIn = new ByteCountingInputStream(bufferedIn);
            final DataInputStream in = new DataInputStream(byteCountingIn)) {

            try {
                // Validate that the header is what we expect and obtain the appropriate SerDe and Version information
                final SerDeAndVersion serdeAndVersion = validateHeader(in);
                final SerDe<T> serde = serdeAndVersion.getSerDe();

                // Ensure that we get a valid transaction indicator
                int transactionIndicator = in.read();
                if (transactionIndicator != TRANSACTION_FOLLOWS && transactionIndicator != JOURNAL_COMPLETE && transactionIndicator != -1) {
                    throw new IOException("After reading " + byteCountingIn.getBytesConsumed() + " bytes from " + journalFile + ", encountered unexpected value of "
                        + transactionIndicator + " for the Transaction Indicator. This journal may have been corrupted.");
                }

                long consumedAtLog = 0L;

                // We don't want to apply the updates in a transaction until we've finished recovering the entire
                // transaction. Otherwise, we could apply say 8 out of 10 updates and then hit an EOF. In such a case,
                // we want to rollback the entire transaction. We handle this by not updating recordMap or swapLocations
                // variables directly but instead keeping track of the things that occurred and then once we've read the
                // entire transaction, we can apply those updates to the recordMap and swapLocations.
                final Map<Object, T> transactionRecordMap = new HashMap<>();
                final Set<Object> idsRemoved = new HashSet<>();
                final Set<String> swapLocationsRemoved = new HashSet<>();
                final Set<String> swapLocationsAdded = new HashSet<>();
                int transactionUpdates = 0;

                // While we have a transaction to recover, recover it
                while (transactionIndicator == TRANSACTION_FOLLOWS) {
                    transactionRecordMap.clear();
                    idsRemoved.clear();
                    swapLocationsRemoved.clear();
                    swapLocationsAdded.clear();
                    transactionUpdates = 0;

                    // Format is <Transaction ID: 8 bytes> <Transaction Length: 4 bytes> <Transaction data: # of bytes indicated by Transaction Length Field>
                    final long transactionId = in.readLong();
                    maxTransactionId = Math.max(maxTransactionId, transactionId);
                    final int transactionLength = in.readInt();

                    // Use SerDe to deserialize the update. We use a LimitingInputStream to ensure that the SerDe is not able to read past its intended
                    // length, in case there is a bug in the SerDe. We then use a ByteCountingInputStream so that we can ensure that all of the data has
                    // been read and throw EOFException otherwise.
                    final InputStream transactionLimitingIn = new LimitingInputStream(in, transactionLength);
                    final ByteCountingInputStream transactionByteCountingIn = new ByteCountingInputStream(transactionLimitingIn);
                    final DataInputStream transactionDis = new DataInputStream(transactionByteCountingIn);

                    while (transactionByteCountingIn.getBytesConsumed() < transactionLength || serde.isMoreInExternalFile()) {
                        final T record = serde.deserializeEdit(transactionDis, recordMap, serdeAndVersion.getVersion());

                        // Update our RecordMap so that we have the most up-to-date version of the Record.
                        final Object recordId = serde.getRecordIdentifier(record);
                        final UpdateType updateType = serde.getUpdateType(record);

                        switch (updateType) {
                            case DELETE: {
                                idsRemoved.add(recordId);
                                transactionRecordMap.remove(recordId);
                                break;
                            }
                            case SWAP_IN: {
                                final String location = serde.getLocation(record);
                                if (location == null) {
                                    logger.error("Recovered SWAP_IN record from edit log, but it did not contain a Location; skipping record");
                                } else {
                                    swapLocationsRemoved.add(location);
                                    swapLocationsAdded.remove(location);
                                    transactionRecordMap.put(recordId, record);
                                }
                                break;
                            }
                            case SWAP_OUT: {
                                final String location = serde.getLocation(record);
                                if (location == null) {
                                    logger.error("Recovered SWAP_OUT record from edit log, but it did not contain a Location; skipping record");
                                } else {
                                    swapLocationsRemoved.remove(location);
                                    swapLocationsAdded.add(location);
                                    idsRemoved.add(recordId);
                                    transactionRecordMap.remove(recordId);
                                }

                                break;
                            }
                            default: {
                                transactionRecordMap.put(recordId, record);
                                idsRemoved.remove(recordId);
                                break;
                            }
                        }

                        transactionUpdates++;
                    }

                    // Apply the transaction
                    for (final Object id : idsRemoved) {
                        recordMap.remove(id);
                    }
                    recordMap.putAll(transactionRecordMap);
                    swapLocations.removeAll(swapLocationsRemoved);
                    swapLocations.addAll(swapLocationsAdded);
                    updateCount += transactionUpdates;

                    // Check if there is another transaction to read
                    transactionIndicator = in.read();
                    if (transactionIndicator != TRANSACTION_FOLLOWS && transactionIndicator != JOURNAL_COMPLETE && transactionIndicator != -1) {
                        throw new IOException("After reading " + byteCountingIn.getBytesConsumed() + " bytes from " + journalFile + ", encountered unexpected value of "
                            + transactionIndicator + " for the Transaction Indicator. This journal may have been corrupted.");
                    }

                    // If we have a very large journal (for instance, if checkpoint is not called for a long time, or if there is a problem rolling over
                    // the journal), then we want to occasionally notify the user that we are, in fact, making progress, so that it doesn't appear that
                    // NiFi has become "stuck".
                    final long consumed = byteCountingIn.getBytesConsumed();
                    if (consumed - consumedAtLog > 50_000_000) {
                        final double percentage = consumed / journalLength * 100D;
                        final String pct = new DecimalFormat("#.00").format(percentage);
                        logger.info("{}% of the way finished recovering journal {}, having recovered {} updates", pct, journalFile, updateCount);
                        consumedAtLog = consumed;
                    }
                }
            } catch (final EOFException eof) {
                eofException = true;
                logger.warn("Encountered unexpected End-of-File when reading journal file {}; assuming that NiFi was shutdown unexpectedly and continuing recovery", journalFile);
            } catch (final Exception e) {
                // If the stream consists solely of NUL bytes, then we want to treat it
                // the same as an EOF because we see this happen when we suddenly lose power
                // while writing to a file. However, if that is not the case, then something else has gone wrong.
                // In such a case, there is not much that we can do but to re-throw the Exception.
                if (remainingBytesAllNul(in)) {
                    logger.warn("Failed to recover some of the data from Write-Ahead Log Journal because encountered trailing NUL bytes. "
                        + "This will sometimes happen after a sudden power loss. The rest of this journal file will be skipped for recovery purposes."
                        + "The following Exception was encountered while recovering the updates to the journal:", e);
                } else {
                    throw e;
                }
            }
        }

        logger.info("Successfully recovered {} updates from journal {}", updateCount, journalFile);
        return new StandardJournalRecovery(updateCount, maxTransactionId, eofException);
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


    @Override
    public synchronized JournalSummary getSummary() {
        if (transactionCount < 1) {
            return INACTIVE_JOURNAL_SUMMARY;
        }

        return new StandardJournalSummary(initialTransactionId, currentTransactionId - 1, transactionCount);
    }

    private class SerDeAndVersion {
        private final SerDe<T> serde;
        private final int version;

        public SerDeAndVersion(final SerDe<T> serde, final int version) {
            this.serde = serde;
            this.version = version;
        }

        public SerDe<T> getSerDe() {
            return serde;
        }

        public int getVersion() {
            return version;
        }
    }
}
