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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.provenance.AbstractRecordWriter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompressableRecordWriter extends AbstractRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(CompressableRecordWriter.class);

    private final FileOutputStream fos;
    private final ByteCountingOutputStream rawOutStream;
    private final boolean compressed;
    private final int uncompressedBlockSize;

    private DataOutputStream out;
    private ByteCountingOutputStream byteCountingOut;
    private long lastBlockOffset = 0L;
    private int recordCount = 0;


    public CompressableRecordWriter(final File file, final TocWriter writer, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(file, writer);
        logger.trace("Creating Record Writer for {}", file.getName());

        this.compressed = compressed;
        this.fos = new FileOutputStream(file);
        rawOutStream = new ByteCountingOutputStream(fos);
        this.uncompressedBlockSize = uncompressedBlockSize;
    }

    public CompressableRecordWriter(final OutputStream out, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(null, tocWriter);
        this.fos = null;

        this.compressed = compressed;
        this.uncompressedBlockSize = uncompressedBlockSize;
        this.rawOutStream = new ByteCountingOutputStream(out);
    }


    @Override
    public synchronized void writeHeader(final long firstEventId) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        try {
            lastBlockOffset = rawOutStream.getBytesWritten();
            resetWriteStream(firstEventId);
            out.writeUTF(getSerializationName());
            out.writeInt(getSerializationVersion());
            writeHeader(firstEventId, out);
            out.flush();
            lastBlockOffset = rawOutStream.getBytesWritten();
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
    private void resetWriteStream(final long eventId) throws IOException {
        try {
            if (out != null) {
                out.flush();
            }

            final long byteOffset = (byteCountingOut == null) ? rawOutStream.getBytesWritten() : byteCountingOut.getBytesWritten();
            final TocWriter tocWriter = getTocWriter();

            final OutputStream writableStream;
            if (compressed) {
                // because of the way that GZIPOutputStream works, we need to call close() on it in order for it
                // to write its trailing bytes. But we don't want to close the underlying OutputStream, so we wrap
                // the underlying OutputStream in a NonCloseableOutputStream
                // We don't have to check if the writer is dirty because we will have already checked before calling this method.
                if (out != null) {
                    out.close();
                }

                if (tocWriter != null) {
                    tocWriter.addBlockOffset(rawOutStream.getBytesWritten(), eventId);
                }

                writableStream = new BufferedOutputStream(new GZIPOutputStream(new NonCloseableOutputStream(rawOutStream), 1), 65536);
            } else {
                if (tocWriter != null) {
                    tocWriter.addBlockOffset(rawOutStream.getBytesWritten(), eventId);
                }

                writableStream = new BufferedOutputStream(rawOutStream, 65536);
            }

            this.byteCountingOut = new ByteCountingOutputStream(writableStream, byteOffset);
            this.out = new DataOutputStream(byteCountingOut);
            resetDirtyFlag();
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }



    @Override
    public long writeRecord(final ProvenanceEventRecord record, final long recordIdentifier) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        try {
            final long startBytes = byteCountingOut.getBytesWritten();

            // add a new block to the TOC if needed.
            if (getTocWriter() != null && (startBytes - lastBlockOffset >= uncompressedBlockSize)) {
                lastBlockOffset = startBytes;

                if (compressed) {
                    // because of the way that GZIPOutputStream works, we need to call close() on it in order for it
                    // to write its trailing bytes. But we don't want to close the underlying OutputStream, so we wrap
                    // the underlying OutputStream in a NonCloseableOutputStream
                    resetWriteStream(recordIdentifier);
                }
            }

            writeRecord(record, recordIdentifier, out);

            recordCount++;
            return byteCountingOut.getBytesWritten() - startBytes;
        } catch (final IOException ioe) {
            markDirty();
            throw ioe;
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public synchronized int getRecordsWritten() {
        return recordCount;
    }

    @Override
    protected OutputStream getBufferedOutputStream() {
        return out;
    }

    @Override
    protected OutputStream getUnderlyingOutputStream() {
        return fos;
    }

    @Override
    protected void syncUnderlyingOutputStream() throws IOException {
        if (fos != null) {
            fos.getFD().sync();
        }
    }

    protected abstract void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException;

    protected abstract void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException;

    protected abstract int getSerializationVersion();

    protected abstract String getSerializationName();
}
