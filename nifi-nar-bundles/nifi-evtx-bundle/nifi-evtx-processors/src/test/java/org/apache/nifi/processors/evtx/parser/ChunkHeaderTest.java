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
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;
import org.apache.nifi.processors.evtx.parser.bxml.EndOfStreamNode;
import org.apache.nifi.processors.evtx.parser.bxml.NameStringNode;
import org.apache.nifi.processors.evtx.parser.bxml.NameStringNodeTest;
import org.apache.nifi.processors.evtx.parser.bxml.RootNode;
import org.apache.nifi.processors.evtx.parser.bxml.TemplateNode;
import org.apache.nifi.processors.evtx.parser.bxml.TemplateNodeTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ChunkHeaderTest {
    private final Random random = new Random();
    private int headerOffset = 101;
    private int chunkNumber = 102;
    private ChunkHeader chunkHeader;
    private int fileFirstRecordNumber = 103;
    private int fileLastRecordNumber = 105;
    private int logFirstRecordNumber = 106;
    private int logLastRecordNumber = 107;
    private int headerSize = 112;
    private int lastRecordOffset = 12380;
    private int nextRecordOffset = 2380;
    private int dataChecksum = 1111;
    private List<String> guids;

    @Before
    public void setup() throws IOException {
        TestBinaryReaderBuilder testBinaryReaderBuilder = new TestBinaryReaderBuilder();
        testBinaryReaderBuilder.putString(ChunkHeader.ELF_CHNK);
        testBinaryReaderBuilder.putQWord(fileFirstRecordNumber);
        testBinaryReaderBuilder.putQWord(fileLastRecordNumber);
        testBinaryReaderBuilder.putQWord(logFirstRecordNumber);
        testBinaryReaderBuilder.putQWord(logLastRecordNumber);
        testBinaryReaderBuilder.putDWord(headerSize);
        testBinaryReaderBuilder.putDWord(lastRecordOffset);
        testBinaryReaderBuilder.putDWord(nextRecordOffset);
        testBinaryReaderBuilder.putDWord(dataChecksum);
        byte[] bytes = new byte[67];
        random.nextBytes(bytes);
        testBinaryReaderBuilder.put(bytes);
        testBinaryReaderBuilder.put((byte) 0);

        // Checksum placeholder
        testBinaryReaderBuilder.putDWord(0);

        TestBinaryReaderBuilder dataBuilder = new TestBinaryReaderBuilder();
        int offset = 545;
        for (int i = 0; i < 64; i++) {
            String string = Integer.toString(i);
            testBinaryReaderBuilder.putDWord(offset);
            offset += NameStringNodeTest.putNode(dataBuilder, 0, string.hashCode(), string);
        }

        guids = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            testBinaryReaderBuilder.putDWord(offset + 10);
            dataBuilder.put((byte) 0x0C);
            dataBuilder.put(new byte[5]);
            dataBuilder.putDWord(offset + 10);
            byte[] guidBytes = new byte[16];
            random.nextBytes(guidBytes);
            String guid = new TestBinaryReaderBuilder().put(guidBytes).build().readGuid();
            guids.add(guid);
            offset += TemplateNodeTest.putNode(dataBuilder, 0, guid, i);
            dataBuilder.put((byte) BxmlNode.END_OF_STREAM_TOKEN);
            offset += 11;
        }

        RecordTest.putNode(testBinaryReaderBuilder, fileLastRecordNumber, new Date());

        testBinaryReaderBuilder.put(dataBuilder.toByteArray());

        CRC32 dataChecksum = new CRC32();
        dataChecksum.update(testBinaryReaderBuilder.toByteArray(), 512, nextRecordOffset - 512);
        testBinaryReaderBuilder.putDWordAt(UnsignedInteger.valueOf(dataChecksum.getValue()).intValue(), 52);

        CRC32 headerChecksum = new CRC32();
        byte[] array = testBinaryReaderBuilder.toByteArray();
        headerChecksum.update(array, 0, 120);
        headerChecksum.update(array, 128, 384);
        testBinaryReaderBuilder.putDWordAt(UnsignedInteger.valueOf(headerChecksum.getValue()).intValue(), 124);
        chunkHeader = new ChunkHeader(testBinaryReaderBuilder.build(), mock(ComponentLog.class), headerOffset, chunkNumber);
    }

    @Test
    public void testInit() throws IOException {
        int count = 0;
        for (Map.Entry<Integer, NameStringNode> integerNameStringNodeEntry : new TreeMap<>(chunkHeader.getNameStrings()).entrySet()) {
            assertEquals(Integer.toString(count++), integerNameStringNodeEntry.getValue().getString());
        }

        Iterator<String> iterator = guids.iterator();
        for (Map.Entry<Integer, TemplateNode> integerTemplateNodeEntry : new TreeMap<>(chunkHeader.getTemplateNodes()).entrySet()) {
            assertEquals(iterator.next(), integerTemplateNodeEntry.getValue().getGuid());
        }

        assertTrue(chunkHeader.hasNext());

        Record next = chunkHeader.next();
        assertEquals(fileLastRecordNumber, next.getRecordNum().intValue());
        RootNode rootNode = next.getRootNode();
        List<BxmlNode> children = rootNode.getChildren();
        assertEquals(1, children.size());
        assertTrue(children.get(0) instanceof EndOfStreamNode);
        assertEquals(0, rootNode.getSubstitutions().size());

        assertFalse(chunkHeader.hasNext());
    }
}
