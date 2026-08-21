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
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class FileHeaderTest {
    private final Random random = new Random();
    private FileHeader fileHeader;
    private final int oldestChunk = 111;
    private final int currentChunkNumber = 112;
    private final int nextRecordNumber = 113;
    private final int headerSize = 114;
    private final int minorVersion = 1;
    private final int majorVersion = 3;
    private final int headerChunkSize = 4096;
    private final int chunkCount = 0;
    private final int flags = 340942;

    @BeforeEach
    public void setup() throws IOException {
        TestBinaryReaderBuilder testBinaryReaderBuilder = new TestBinaryReaderBuilder();
        testBinaryReaderBuilder.putString(FileHeader.ELF_FILE);
        testBinaryReaderBuilder.putQWord(oldestChunk);
        testBinaryReaderBuilder.putQWord(currentChunkNumber);
        testBinaryReaderBuilder.putQWord(nextRecordNumber);
        testBinaryReaderBuilder.putDWord(headerSize);
        testBinaryReaderBuilder.putWord(minorVersion);
        testBinaryReaderBuilder.putWord(majorVersion);
        testBinaryReaderBuilder.putWord(headerChunkSize);
        testBinaryReaderBuilder.putWord(chunkCount);
        byte[] unused = new byte[75];
        random.nextBytes(unused);
        testBinaryReaderBuilder.put(unused);
        testBinaryReaderBuilder.put((byte) 0);

        CRC32 crc32 = new CRC32();
        crc32.update(testBinaryReaderBuilder.toByteArray());

        testBinaryReaderBuilder.putDWord(flags);
        testBinaryReaderBuilder.putDWord(UnsignedInteger.valueOf(crc32.getValue()));

        fileHeader = new FileHeader(new ByteArrayInputStream(testBinaryReaderBuilder.toByteArray(4096)), mock(ComponentLog.class));
    }

    @Test
    public void testInit() {
        assertEquals(FileHeader.ELF_FILE, fileHeader.getMagicString());
        assertEquals(oldestChunk, fileHeader.getOldestChunk().intValue());
        assertEquals(currentChunkNumber, fileHeader.getCurrentChunkNumber().intValue());
        assertEquals(nextRecordNumber, fileHeader.getNextRecordNumber().intValue());
        assertEquals(headerSize, fileHeader.getHeaderSize().intValue());
        assertEquals(minorVersion, fileHeader.getMinorVersion());
        assertEquals(majorVersion, fileHeader.getMajorVersion());
        assertEquals(headerChunkSize, fileHeader.getHeaderChunkSize());
        assertEquals(chunkCount, fileHeader.getChunkCount());
        assertEquals(flags, fileHeader.getFlags().intValue());
    }
}
