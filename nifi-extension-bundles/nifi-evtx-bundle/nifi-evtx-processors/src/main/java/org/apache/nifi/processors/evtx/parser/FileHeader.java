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

package org.apache.nifi.processors.evtx.parser;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

/**
 * FileHeader at the top of an Evtx file has metadata about the chunks of the file as well as top-level metadata
 */
public class FileHeader extends Block {
    public static final int CHUNK_SIZE = 65536;
    public static final String ELF_FILE = "ElfFile";
    private final String magicString;
    private final UnsignedLong oldestChunk;
    private final UnsignedLong currentChunkNumber;
    private final UnsignedLong nextRecordNumber;
    private final UnsignedInteger headerSize;
    private final int minorVersion;
    private final int majorVersion;
    private final int headerChunkSize;
    private final int chunkCount;
    private final String unused1;
    private final UnsignedInteger flags;
    private final UnsignedInteger checksum;
    private final InputStream inputStream;
    private final ComponentLog log;
    private long currentOffset;
    private int count = 1;

    public FileHeader(InputStream inputStream, ComponentLog log) throws IOException {
        super(new BinaryReader(inputStream, 4096));
        this.log = log;
        // Bytes will be checksummed
        BinaryReader binaryReader = getBinaryReader();
        CRC32 crc32 = new CRC32();
        crc32.update(binaryReader.peekBytes(120));

        magicString = binaryReader.readString(8);
        if (!ELF_FILE.equals(magicString)) {
            throw new IOException("Invalid magic string. Expected " + ELF_FILE + " got " + magicString);
        }
        oldestChunk = binaryReader.readQWord();
        currentChunkNumber = binaryReader.readQWord();
        nextRecordNumber = binaryReader.readQWord();
        headerSize = binaryReader.readDWord();
        minorVersion = binaryReader.readWord();
        majorVersion = binaryReader.readWord();
        headerChunkSize = binaryReader.readWord();
        chunkCount = binaryReader.readWord();
        unused1 = binaryReader.readString(76);

        // Not part of checksum
        flags = binaryReader.readDWord();
        checksum = binaryReader.readDWord();

        if (crc32.getValue() != checksum.longValue()) {
            throw new IOException("Invalid checksum");
        }
        NumberUtil.intValueExpected(minorVersion, 1, "Invalid minor version.");
        NumberUtil.intValueExpected(majorVersion, 3, "Invalid minor version.");
        NumberUtil.intValueExpected(headerChunkSize, 4096, "Invalid header chunk size.");
        this.inputStream = inputStream;
        currentOffset = 4096;

        init();
    }

    @Override
    protected int getHeaderLength() {
        return 4096;
    }

    public String getMagicString() {
        return magicString;
    }

    public UnsignedLong getOldestChunk() {
        return oldestChunk;
    }

    public UnsignedLong getCurrentChunkNumber() {
        return currentChunkNumber;
    }

    public UnsignedLong getNextRecordNumber() {
        return nextRecordNumber;
    }

    public UnsignedInteger getHeaderSize() {
        return headerSize;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getHeaderChunkSize() {
        return headerChunkSize;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public String getUnused1() {
        return unused1;
    }

    public UnsignedInteger getFlags() {
        return flags;
    }

    public UnsignedInteger getChecksum() {
        return checksum;
    }

    /**
     * Tests whether there are more chunks
     * @return true if there are chunks left
     */
    public boolean hasNext() {
        return count <= chunkCount;
    }

    /**
     * Returns the next chunkHeader or null if there are no more
     *
     * @return chunkHeader
     * @throws MalformedChunkException if there is an error reading the chunk header
     * @throws IOException if there is an exception creating the BinaryReader
     */
    public ChunkHeader next() throws MalformedChunkException, IOException {
        if (count <= chunkCount) {
            long currentOffset = this.currentOffset;
            this.currentOffset += CHUNK_SIZE;
            BinaryReader binaryReader = new BinaryReader(inputStream, CHUNK_SIZE);
            try {
                return new ChunkHeader(binaryReader, log, currentOffset, count++);
            } catch (IOException e) {
                throw new MalformedChunkException("Malformed chunk, unable to parse", e, currentOffset, count - 1, binaryReader.getBytes());
            }
        } else {
            return null;
        }
    }
}
