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

import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProcessingInstructionDataNodeTest extends BxmlNodeWithTokenTestBase {
    private String data = "testData";
    private ProcessingInstructionDataNode processingInstructionDataNode;

    @Override
    public void setup() throws IOException {
        super.setup();
        testBinaryReaderBuilder.putWord(data.length());
        testBinaryReaderBuilder.putWString(data);
        processingInstructionDataNode = new ProcessingInstructionDataNode(testBinaryReaderBuilder.build(), chunkHeader, parent);
    }

    @Test
    public void testInit() {
        assertEquals(getToken(), processingInstructionDataNode.getToken());
        assertEquals(data + "?>", processingInstructionDataNode.getValue());
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        processingInstructionDataNode.accept(mock);
        verify(mock).visit(processingInstructionDataNode);
        verifyNoMoreInteractions(mock);
    }

    @Test
    public void testNoData() throws IOException {
        super.setup();
        testBinaryReaderBuilder.putDWord(0);
        processingInstructionDataNode = new ProcessingInstructionDataNode(testBinaryReaderBuilder.build(), chunkHeader, parent);

        assertEquals(getToken(), processingInstructionDataNode.getToken());
        assertEquals("?>", processingInstructionDataNode.getValue());
    }

    @Override
    protected byte getToken() {
        return BxmlNode.PROCESSING_INSTRUCTION_DATA_TOKEN;
    }
}
