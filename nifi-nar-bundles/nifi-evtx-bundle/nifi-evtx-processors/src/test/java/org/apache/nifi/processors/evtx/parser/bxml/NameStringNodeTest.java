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

import com.google.common.primitives.UnsignedInteger;
import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.apache.nifi.processors.evtx.parser.TestBinaryReaderBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class NameStringNodeTest extends BxmlNodeTestBase {
    private int nextOffset = 300;
    private int hash = 100;
    private String string = "test string";
    private NameStringNode nameStringNode;

    public static int putNode(TestBinaryReaderBuilder testBinaryReaderBuilder, int nextOffset, int hash, String string) throws IOException {
        testBinaryReaderBuilder.putDWord(nextOffset);
        testBinaryReaderBuilder.putWord(hash);
        testBinaryReaderBuilder.putWord(string.length());
        testBinaryReaderBuilder.putWString(string);
        return 8 + (2 * string.length());
    }

    @Override
    public void setup() throws IOException {
        super.setup();
        putNode(testBinaryReaderBuilder, nextOffset, hash, string);
        nameStringNode = new NameStringNode(testBinaryReaderBuilder.build(), chunkHeader);
    }

    @Test
    public void testInit() {
        assertEquals(UnsignedInteger.valueOf(nextOffset), nameStringNode.getNextOffset());
        assertEquals(hash, nameStringNode.getHash());
        assertEquals(string, nameStringNode.getString());
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        nameStringNode.accept(mock);
        verify(mock).visit(nameStringNode);
        verifyNoMoreInteractions(mock);
    }
}
