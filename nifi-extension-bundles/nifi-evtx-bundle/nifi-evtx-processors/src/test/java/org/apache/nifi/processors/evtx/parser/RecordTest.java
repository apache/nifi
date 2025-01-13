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

import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;
import org.apache.nifi.processors.evtx.parser.bxml.EndOfStreamNode;
import org.apache.nifi.processors.evtx.parser.bxml.RootNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@ExtendWith(MockitoExtension.class)
public class RecordTest {
    @Mock
    ChunkHeader chunkHeader;

    private Record record;
    private int recordNum = 120;

    public static void putNode(TestBinaryReaderBuilder testBinaryReaderBuilder, int recordNum, Date fileTime) {
        testBinaryReaderBuilder.putDWord(10794);
        int size = 20;
        testBinaryReaderBuilder.putDWord(size);
        testBinaryReaderBuilder.putQWord(recordNum);
        testBinaryReaderBuilder.putFileTime(new Date());

        testBinaryReaderBuilder.put((byte) BxmlNode.END_OF_STREAM_TOKEN);
        testBinaryReaderBuilder.putDWord(0);
        testBinaryReaderBuilder.putDWord(size);
    }

    @BeforeEach
    public void setup() throws IOException {
        TestBinaryReaderBuilder testBinaryReaderBuilder = new TestBinaryReaderBuilder();
        putNode(testBinaryReaderBuilder, recordNum, new Date());

        record = new Record(testBinaryReaderBuilder.build(), chunkHeader);
    }

    @Test
    public void testInit() {
        assertEquals(recordNum, record.getRecordNum().intValue());
        RootNode rootNode = record.getRootNode();
        List<BxmlNode> children = rootNode.getChildren();
        assertEquals(1, children.size());
        assertInstanceOf(EndOfStreamNode.class, children.get(0));
        assertEquals(0, rootNode.getSubstitutions().size());
    }
}
