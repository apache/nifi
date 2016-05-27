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

package org.apache.nifi.processors.evtx.parser.bxml;

import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.apache.nifi.processors.evtx.parser.TestBinaryReaderBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OpenStartElementNodeTest extends BxmlNodeWithTokenTestBase {
    private int unknown = 24;
    private int size = 2444;
    private int stringOffset = 0;
    private String tagName = "tagName";
    private OpenStartElementNode openStartElementNode;

    @Override
    public void setup() throws IOException {
        super.setup();
        testBinaryReaderBuilder.putWord(unknown);
        testBinaryReaderBuilder.putDWord(size);
        testBinaryReaderBuilder.putDWord(stringOffset);

        testBinaryReaderBuilder.put((byte) BxmlNode.CLOSE_EMPTY_ELEMENT_TOKEN);
        when(chunkHeader.getString(stringOffset)).thenReturn(tagName);
        openStartElementNode = new OpenStartElementNode(testBinaryReaderBuilder.build(), chunkHeader, parent);
    }

    @Test
    public void testInit() {
        assertEquals(tagName, openStartElementNode.getTagName());
        List<BxmlNode> children = openStartElementNode.getChildren();
        assertEquals(1, children.size());
        assertTrue(children.get(0) instanceof CloseEmptyElementNode);
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        openStartElementNode.accept(mock);
        verify(mock).visit(openStartElementNode);
        verifyNoMoreInteractions(mock);
    }

    @Test
    public void testWithFlagAndEmbeddedNameStringNode() throws IOException {
        byte token = (byte) (0x04 << 4 | getToken());
        stringOffset = 5;
        tagName = "teststring";
        testBinaryReaderBuilder = new TestBinaryReaderBuilder();
        testBinaryReaderBuilder.put(token);
        testBinaryReaderBuilder.putWord(unknown);
        testBinaryReaderBuilder.putDWord(size);
        testBinaryReaderBuilder.putDWord(stringOffset);

        testBinaryReaderBuilder.putDWord(0);
        testBinaryReaderBuilder.putWord(0);
        testBinaryReaderBuilder.putWord(tagName.length());
        testBinaryReaderBuilder.putWString(tagName);
        testBinaryReaderBuilder.putWord(0);
        testBinaryReaderBuilder.put(new byte[5]);

        testBinaryReaderBuilder.put((byte) BxmlNode.CLOSE_EMPTY_ELEMENT_TOKEN);

        BinaryReader binaryReader = testBinaryReaderBuilder.build();
        NameStringNode nameStringNode = mock(NameStringNode.class);
        when(nameStringNode.getString()).thenReturn(tagName);
        when(chunkHeader.addNameStringNode(stringOffset, binaryReader)).thenAnswer(invocation -> new NameStringNode(binaryReader, chunkHeader));
        openStartElementNode = new OpenStartElementNode(binaryReader, chunkHeader, parent);

        assertEquals(getToken(), openStartElementNode.getToken() & 0x0F);
        assertTrue((openStartElementNode.getFlags() & 0x04) > 0);
        assertEquals(tagName, openStartElementNode.getTagName());
    }

    @Override
    protected byte getToken() {
        return BxmlNode.OPEN_START_ELEMENT_TOKEN;
    }
}
