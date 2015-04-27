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

            final int numBlocks = (int) ((file.length() - 2) / 8);
            offsets = new long[numBlocks];

            for (int i=0; i < numBlocks; i++) {
                offsets[i] = dis.readLong();
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
                return i-1;
            }
        }

        return offsets.length - 1;
    }

}
