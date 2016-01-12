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
package org.apache.nifi.provenance.toc;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Standard implementation of TocReader.
 *
 * Expects .toc file to be in the following format;
 *
 * byte 0: version
 * byte 1: boolean: compressionFlag -> 0 = journal is NOT compressed, 1 = journal is compressed
 * byte 2-9: long: offset of block 0
 * byte 10-17: long: offset of block 1
 * ...
 * byte (N*8+2)-(N*8+9): long: offset of block N
 */
public class StandardTocReader implements TocReader {
    private final boolean compressed;
    private final long[] offsets;
    private final long[] firstEventIds;

    public StandardTocReader(final File file) throws IOException {
        try (final FileInputStream fis = new FileInputStream(file);
                final DataInputStream dis = new DataInputStream(fis)) {

            final int version = dis.read();
            if ( version < 0 ) {
                throw new EOFException();
            }

            final int compressionFlag = dis.read();
            if ( compressionFlag < 0 ) {
                throw new EOFException();
            }

            if ( compressionFlag == 0 ) {
                compressed = false;
            } else if ( compressionFlag == 1 ) {
                compressed = true;
            } else {
                throw new IOException("Table of Contents appears to be corrupt: could not read 'compression flag' from header; expected value of 0 or 1 but got " + compressionFlag);
            }

            final int blockInfoBytes;
            switch (version) {
                case 1:
                    blockInfoBytes = 8;
                    break;
                case 2:
                default:
                    blockInfoBytes = 16;
                    break;
            }

            final int numBlocks = (int) ((file.length() - 2) / blockInfoBytes);
            offsets = new long[numBlocks];

            if ( version > 1 ) {
                firstEventIds = new long[numBlocks];
            } else {
                firstEventIds = new long[0];
            }

            for (int i=0; i < numBlocks; i++) {
                offsets[i] = dis.readLong();

                if ( version > 1 ) {
                    firstEventIds[i] = dis.readLong();
                }
            }
        }
    }

    @Override
    public boolean isCompressed() {
        return compressed;
    }

    @Override
    public long getBlockOffset(final int blockIndex) {
        if ( blockIndex >= offsets.length ) {
            return -1L;
        }
        return offsets[blockIndex];
    }

    @Override
    public long getLastBlockOffset() {
        if ( offsets.length == 0 ) {
            return 0L;
        }
        return offsets[offsets.length - 1];
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int getBlockIndex(final long blockOffset) {
        for (int i=0; i < offsets.length; i++) {
            if ( offsets[i] > blockOffset ) {
                // if the offset is less than the offset of our first block,
                // just return 0 to indicate the first block. Otherwise,
                // return i-1 because i represents the first block whose offset is
                // greater than 'blockOffset'.
                return (i == 0) ? 0 : i-1;
            }
        }

        // None of the blocks have an offset greater than the provided offset.
        // Therefore, if the event is present, it must be in the last block.
        return offsets.length - 1;
    }

    @Override
    public Integer getBlockIndexForEventId(final long eventId) {
        // if we don't have event ID's stored in the TOC (which happens for version 1 of the TOC),
        // or if the event ID is less than the first Event ID in this TOC, then the Event ID
        // is unknown -- return null.
        if ( firstEventIds.length == 0 || eventId < firstEventIds[0] ) {
            return null;
        }

        for (int i=1; i < firstEventIds.length; i++) {
            if ( firstEventIds[i] > eventId ) {
                return i-1;
            }
        }

        // None of the blocks start with an Event ID greater than the provided ID.
        // Therefore, if the event is present, it must be in the last block.
        return firstEventIds.length - 1;
    }
}
