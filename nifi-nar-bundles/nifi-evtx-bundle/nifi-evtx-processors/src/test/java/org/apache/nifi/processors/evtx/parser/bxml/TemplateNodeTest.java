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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TemplateNodeTest extends BxmlNodeTestBase {

    private int nextOffset = 101;
    private String guid = "33323130-3534-3736-3839-616263646566";
    private int dataLength = 102;
    private TemplateNode templateNode;

    public static int putNode(TestBinaryReaderBuilder testBinaryReaderBuilder, int nextOffset, String guid, int dataLength) {
        testBinaryReaderBuilder.putDWord(nextOffset);
        testBinaryReaderBuilder.putGuid(guid);
        testBinaryReaderBuilder.putDWord(dataLength);
        return 24;
    }

    @Override
    public void setup() throws IOException {
        super.setup();
        putNode(testBinaryReaderBuilder, nextOffset, guid, dataLength);
        testBinaryReaderBuilder.put((byte) BxmlNode.END_OF_STREAM_TOKEN);
        templateNode = new TemplateNode(testBinaryReaderBuilder.build(), chunkHeader);
    }

    @Test
    public void testInit() {
        assertEquals(nextOffset, templateNode.getNextOffset());
        assertEquals(UnsignedInteger.valueOf(858927408), templateNode.getTemplateId());
        assertEquals(guid, templateNode.getGuid());
        assertEquals(dataLength, templateNode.getDataLength());
        assertTrue(templateNode.hasEndOfStream());
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        templateNode.accept(mock);
        verify(mock).visit(templateNode);
        verifyNoMoreInteractions(mock);
    }
}
