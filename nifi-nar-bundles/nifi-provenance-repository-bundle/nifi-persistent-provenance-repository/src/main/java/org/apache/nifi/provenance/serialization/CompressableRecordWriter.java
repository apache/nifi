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

package org.apache.nifi.provenance.serialization;

import org.apache.nifi.provenance.AbstractRecordWriter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CompressableRecordWriter extends AbstractRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(CompressableRecordWriter.class);

    private final FileOutputStream fos;
    private final ByteCountingOutputStream rawOutStream;
    private final boolean compressed;
    private final int uncompressedBlockSize;
    private final AtomicLong idGenerator;

    private DataOutputStream out;
    private ByteCountingOutputStream byteCountingOut;
    private long blockStartOffset = 0L;
    private final AtomicInteger recordCount =  new AtomicInteger(0);

    private final AtomicLong bytesWritten = new AtomicLong(0L);

    public CompressableRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
        final int uncompressedBlockSize) throws IOException {
        super(file, writer);
        logger.trace("Creating Record Writer for {}", file.getName());

        this.compressed = compressed;
        this.fos = new FileOutputStream(file);
        rawOutStream = new ByteCountingOutputStream(new BufferedOutputStream(fos)) {
            @Override
            public void write(final int b) throws IOException {
                super.write(b);
                bytesWritten.incrementAndGet();
            }

            @Override
            public void write(final byte[] b) throws IOException {
                super.write(b);
                bytesWritten.addAndGet(b.length);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                super.write(b, off, len);
                bytesWritten.addAndGet(len);
            }
        };

        this.uncompressedBlockSize = uncompressedBlockSize;
        this.idGenerator = idGenerator;
    }

    public CompressableRecordWriter(final OutputStream out, final String storageLocation, final AtomicLong idGenerator, final TocWriter tocWriter, final boolean compressed,
        final int uncompressedBlockSize) throws IOException {
        super(storageLocation, tocWriter);
        this.fos = null;

        this.compressed = compressed;
        this.uncompressedBlockSize = uncompressedBlockSize;
        this.rawOutStream = new ByteCountingOutputStream(new BufferedOutputStream(out));
        this.idGenerator = idGenerator;
    }


    protected AtomicLong getIdGenerator() {
        return idGenerator;
    }

    @Override
    public synchronized void writeHeader(final long firstEventId) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        try {
            blockStartOffset = rawOutStream.getBytesWritten();
            resetWriteStream(firstEventId);
            out.writeUTF(getSerializationName());
            out.writeInt(getSerializationVersion());
            writeHeader(firstEventId, out);
            out.flush();
            blockStartOffset = getBytesWritten();
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }



    /**
     * Resets the streams to prepare for a new block
     *
     * @param eventId the first id that will be written to the new block
     * @throws IOException if unable to flush/close the current streams properly
     */
    protected void resetWriteStream(final Long eventId) throws IOException {
        try {
            if (out != null) {
                out.flush();
            }

            final long byteOffset = (byteCountingOut == null) ? rawOutStream.getBytesWritten() : byteCountingOut.getBytesWritten();
            final TocWriter tocWriter = getTocWriter();

            if (compressed) {
                // because of the way that GZIPOutputStream works, we need to call close() on it in order for it
                // to write its trailing bytes. But we don't want to close the underlying OutputStream, so we wrap
                // the underlying OutputStream in a NonCloseableOutputStream
                // We don't have to check if the writer is dirty because we will have already checked before calling this method.
                if (out != null) {
                    out.close();
                }

                if (tocWriter != null && eventId != null) {
                    tocWriter.addBlockOffset(rawOutStream.getBytesWritten(), eventId);
                }

                final OutputStream writableStream = new BufferedOutputStream(new GZIPOutputStream(new NonCloseableOutputStream(rawOutStream), 1), 65536);
                this.byteCountingOut = new ByteCountingOutputStream(writableStream, byteOffset);
            } else {
                if (tocWriter != null && eventId != null) {
                    tocWriter.addBlockOffset(rawOutStream.getBytesWritten(), eventId);
                }

                this.byteCountingOut = rawOutStream;
            }

            this.out = new DataOutputStream(byteCountingOut);
            resetDirtyFlag();
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }

    protected synchronized void ensureStreamState(final long recordIdentifier, final long startBytes) throws IOException {
        // add a new block to the TOC if needed.
        if (getTocWriter() != null && (startBytes - blockStartOffset >= uncompressedBlockSize)) {
            blockStartOffset = startBytes;
            resetWriteStream(recordIdentifier);
        }
    }

    @Override
    public synchronized StorageSummary writeRecord(final ProvenanceEventRecord record) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        try {
            final long recordIdentifier = record.getEventId() == -1L ? idGenerator.getAndIncrement() : record.getEventId();
            final long startBytes = byteCountingOut.getBytesWritten();

            ensureStreamState(recordIdentifier, startBytes);
            writeRecord(record, recordIdentifier, out);

            recordCount.incrementAndGet();
            final long bytesWritten = byteCountingOut.getBytesWritten();
            final long serializedLength = bytesWritten - startBytes;
            final TocWriter tocWriter = getTocWriter();
            final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
            final String storageLocation = getStorageLocation();
            return new StorageSummary(record, recordIdentifier, storageLocation, blockIndex, serializedLength, bytesWritten);
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }

    public synchronized List<StorageSummary> writeRecords(final Iterable<ProvenanceEventRecord> events) throws IOException {
        final List<StorageSummary> summaries = new ArrayList<>();

        for (final ProvenanceEventRecord event : events) {
            summaries.add(writeRecord(event));
        }

        return summaries;
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten.get();
    }

    @Override
    public synchronized void flush() throws IOException {
        out.flush();
    }

    @Override
    public int getRecordsWritten() {
        return recordCount.get();
    }

    @Override
    protected synchronized DataOutputStream getBufferedOutputStream() {
        return out;
    }

    @Override
    protected synchronized OutputStream getUnderlyingOutputStream() {
        return fos;
    }

    @Override
    protected synchronized void syncUnderlyingOutputStream() throws IOException {
        if (fos != null) {
            fos.getFD().sync();
        }
    }

    protected boolean isCompressed() {
        return compressed;
    }

    protected abstract void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException;

    protected abstract void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException;

    protected abstract int getSerializationVersion();

    protected abstract String getSerializationName();
}
