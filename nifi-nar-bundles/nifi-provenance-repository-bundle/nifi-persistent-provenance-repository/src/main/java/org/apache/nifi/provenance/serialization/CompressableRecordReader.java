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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompressableRecordReader implements RecordReader {
    private static final Logger logger = LoggerFactory.getLogger(CompressableRecordReader.class);

    private final ByteCountingInputStream rawInputStream;
    private final String filename;
    private final int serializationVersion;
    private final boolean compressed;
    private final TocReader tocReader;
    private final int headerLength;
    private final int maxAttributeChars;

    private DataInputStream dis;
    private ByteCountingInputStream byteCountingIn;
    private StandardProvenanceEventRecord pushbackEvent = null;

    public CompressableRecordReader(final InputStream in, final String filename, final int maxAttributeChars) throws IOException {
        this(in, filename, null, maxAttributeChars);
    }

    public CompressableRecordReader(final InputStream in, final String filename, final TocReader tocReader, final int maxAttributeChars) throws IOException {
        logger.trace("Creating RecordReader for {}", filename);

        rawInputStream = new ByteCountingInputStream(in);
        this.maxAttributeChars = maxAttributeChars;

        final InputStream limitedStream;
        if (tocReader == null) {
            limitedStream = rawInputStream;
        } else {
            final long offset1 = tocReader.getBlockOffset(1);
            if (offset1 < 0) {
                limitedStream = rawInputStream;
            } else {
                limitedStream = new LimitingInputStream(rawInputStream, offset1 - rawInputStream.getBytesConsumed());
            }
        }

        final InputStream readableStream;
        if (filename.endsWith(".gz")) {
            readableStream = new BufferedInputStream(new GZIPInputStream(limitedStream));
            compressed = true;
        } else {
            readableStream = new BufferedInputStream(limitedStream);
            compressed = false;
        }

        byteCountingIn = new ByteCountingInputStream(readableStream);
        dis = new DataInputStream(byteCountingIn);

        final String repoClassName = dis.readUTF();
        final int serializationVersion = dis.readInt();
        headerLength = repoClassName.getBytes(StandardCharsets.UTF_8).length + 2 + 4; // 2 bytes for string length, 4 for integer.

        this.serializationVersion = serializationVersion;
        this.filename = filename;
        this.tocReader = tocReader;

        readHeader(dis, serializationVersion);
    }

    @Override
    public void skipToBlock(final int blockIndex) throws IOException {
        if (tocReader == null) {
            throw new IllegalStateException("Cannot skip to block " + blockIndex + " for Provenance Log " + filename + " because no Table-of-Contents file was found for this Log");
        }

        if (blockIndex < 0) {
            throw new IllegalArgumentException("Cannot skip to block " + blockIndex + " because the value is negative");
        }

        if (blockIndex == getBlockIndex()) {
            return;
        }

        final long offset = tocReader.getBlockOffset(blockIndex);
        if (offset < 0) {
            throw new IOException("Unable to find block " + blockIndex + " in Provenance Log " + filename);
        }

        final long curOffset = rawInputStream.getBytesConsumed();

        final long bytesToSkip = offset - curOffset;
        if (bytesToSkip >= 0) {
            try {
                StreamUtils.skip(rawInputStream, bytesToSkip);
                logger.debug("Skipped stream from offset {} to {} ({} bytes skipped)", curOffset, offset, bytesToSkip);
            } catch (final EOFException eof) {
                throw new EOFException("Attempted to skip to byte offset " + offset + " for " + filename + " but file does not have that many bytes (TOC Reader=" + getTocReader() + ")");
            } catch (final IOException e) {
                throw new IOException("Failed to skip to offset " + offset + " for block " + blockIndex + " of Provenance Log " + filename, e);
            }

            resetStreamForNextBlock();
        }
    }

    private void resetStreamForNextBlock() throws IOException {
        final InputStream limitedStream;
        if (tocReader == null) {
            limitedStream = rawInputStream;
        } else {
            final long offset = tocReader.getBlockOffset(1 + getBlockIndex());
            if (offset < 0) {
                limitedStream = rawInputStream;
            } else {
                limitedStream = new LimitingInputStream(rawInputStream, offset - rawInputStream.getBytesConsumed());
            }
        }

        final InputStream readableStream;
        if (compressed) {
            readableStream = new BufferedInputStream(new GZIPInputStream(limitedStream));
        } else {
            readableStream = new BufferedInputStream(limitedStream);
        }

        byteCountingIn = new ByteCountingInputStream(readableStream, rawInputStream.getBytesConsumed());
        dis = new DataInputStream(byteCountingIn);
    }


    @Override
    public TocReader getTocReader() {
        return tocReader;
    }

    @Override
    public boolean isBlockIndexAvailable() {
        return tocReader != null;
    }

    @Override
    public int getBlockIndex() {
        if (tocReader == null) {
            throw new IllegalStateException("Cannot determine Block Index because no Table-of-Contents could be found for Provenance Log " + filename);
        }

        return tocReader.getBlockIndex(rawInputStream.getBytesConsumed());
    }

    @Override
    public long getBytesConsumed() {
        return byteCountingIn.getBytesConsumed();
    }

    @Override
    public boolean isData() {
        try {
            byteCountingIn.mark(1);
            int nextByte = byteCountingIn.read();
            byteCountingIn.reset();

            if (nextByte < 0) {
                try {
                    resetStreamForNextBlock();
                } catch (final EOFException eof) {
                    return false;
                }

                byteCountingIn.mark(1);
                nextByte = byteCountingIn.read();
                byteCountingIn.reset();
            }

            return nextByte >= 0;
        } catch (final IOException ioe) {
            return false;
        }
    }

    @Override
    public long getMaxEventId() throws IOException {
        if (tocReader != null) {
            final long lastBlockOffset = tocReader.getLastBlockOffset();
            skipToBlock(tocReader.getBlockIndex(lastBlockOffset));
        }

        ProvenanceEventRecord record;
        ProvenanceEventRecord lastRecord = null;
        try {
            while ((record = nextRecord()) != null) {
                lastRecord = record;
            }
        } catch (final EOFException eof) {
            // This can happen if we stop NIFi while the record is being written.
            // This is OK, we just ignore this record. The session will not have been
            // committed, so we can just process the FlowFile again.
        }

        return lastRecord == null ? -1L : lastRecord.getEventId();
    }

    @Override
    public void close() throws IOException {
        logger.trace("Closing Record Reader for {}", filename);

        try {
            dis.close();
        } finally {
            try {
                rawInputStream.close();
            } finally {
                if (tocReader != null) {
                    tocReader.close();
                }
            }
        }
    }

    @Override
    public void skip(final long bytesToSkip) throws IOException {
        StreamUtils.skip(dis, bytesToSkip);
    }

    @Override
    public void skipTo(final long position) throws IOException {
        // we are subtracting headerLength from the number of bytes consumed because we used to
        // consider the offset of the first record "0" - now we consider it whatever position it
        // it really is in the stream.
        final long currentPosition = byteCountingIn.getBytesConsumed() - headerLength;
        if (currentPosition == position) {
            return;
        }
        if (currentPosition > position) {
            throw new IOException("Cannot skip to byte offset " + position + " in stream because already at byte offset " + currentPosition);
        }

        final long toSkip = position - currentPosition;
        StreamUtils.skip(dis, toSkip);
    }

    protected String getFilename() {
        return filename;
    }

    protected int getMaxAttributeLength() {
        return maxAttributeChars;
    }

    @Override
    public StandardProvenanceEventRecord nextRecord() throws IOException {
        if (pushbackEvent != null) {
            final StandardProvenanceEventRecord toReturn = pushbackEvent;
            pushbackEvent = null;
            return toReturn;
        }

        if (isData()) {
            while (true) {
                try {
                    return nextRecord(dis, serializationVersion);
                } catch (final IOException ioe) {
                    throw ioe;
                } catch (final Exception e) {
                    // This would only happen if a bug were to exist such that an 'invalid' event were written
                    // out. For example an Event that has no FlowFile UUID. While there is in fact an underlying
                    // cause that would need to be sorted out in this case, the Provenance Repository should be
                    // resilient enough to handle this. Otherwise, we end up throwing an Exception, which may
                    // prevent iterating over additional events in the repository.
                    logger.error("Failed to read Provenance Event from " + filename + "; will skip this event and continue reading subsequent events", e);
                }
            }
        } else {
            return null;
        }
    }

    protected Optional<Integer> getBlockIndex(final long eventId) {
        final TocReader tocReader = getTocReader();
        if (tocReader == null) {
            return Optional.empty();
        } else {
            final Integer blockIndex = tocReader.getBlockIndexForEventId(eventId);
            return Optional.ofNullable(blockIndex);
        }
    }

    @Override
    public Optional<ProvenanceEventRecord> skipToEvent(final long eventId) throws IOException {
        if (pushbackEvent != null) {
            final StandardProvenanceEventRecord previousPushBack = pushbackEvent;
            if (previousPushBack.getEventId() >= eventId) {
                return Optional.of(previousPushBack);
            } else {
                pushbackEvent = null;
            }
        }

        final Optional<Integer> blockIndex = getBlockIndex(eventId);
        if (blockIndex.isPresent()) {
            // Skip to the appropriate block index and then read until we've found an Event
            // that has an ID >= the event id.
            skipToBlock(blockIndex.get());
        }

        try {
            boolean read = true;
            while (read) {
                final Optional<StandardProvenanceEventRecord> eventOptional = this.readToEvent(eventId, dis, serializationVersion);
                if (eventOptional.isPresent()) {
                    pushbackEvent = eventOptional.get();
                    return Optional.of(pushbackEvent);
                } else {
                    read = isData();
                }
            }

            return Optional.empty();
        } catch (final EOFException eof) {
            // This can occur if we run out of data and attempt to read the next event ID.
            logger.error("Unexpectedly reached end of File when looking for Provenance Event with ID {} in {}", eventId, filename);
            return Optional.empty();
        }
    }

    protected Optional<StandardProvenanceEventRecord> readToEvent(final long eventId, final DataInputStream dis, final int serializationVerison) throws IOException {
        StandardProvenanceEventRecord event;
        while ((event = nextRecord()) != null) {
            if (event.getEventId() >= eventId) {
                return Optional.of(event);
            }
        }

        return Optional.empty();
    }

    protected abstract StandardProvenanceEventRecord nextRecord(DataInputStream in, int serializationVersion) throws IOException;

    protected void readHeader(DataInputStream in, int serializationVersion) throws IOException {
    }
}
