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
import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TemplateInstanceNodeTest extends BxmlNodeWithTokenTestBase {
    private byte unknown = 23;
    private int templateId = 101;
    private int templateLength = 25;
    private TemplateNode templateNode;
    private TemplateInstanceNode templateInstanceNode;

    @Override
    public void setup() throws IOException {
        super.setup();
        testBinaryReaderBuilder.put(unknown);
        testBinaryReaderBuilder.putDWord(templateId);
        testBinaryReaderBuilder.putDWord(0);
        templateNode = mock(TemplateNode.class);
        when(templateNode.getTemplateId()).thenReturn(UnsignedInteger.valueOf(templateId));
        when(chunkHeader.getTemplateNode(0)).thenReturn(templateNode);
        templateInstanceNode = new TemplateInstanceNode(testBinaryReaderBuilder.build(), chunkHeader, parent);
    }

    @Test
    public void testInit() {
        assertEquals(getToken(), templateInstanceNode.getToken());
        assertEquals(10, templateInstanceNode.getHeaderLength());
        assertEquals(templateNode, templateInstanceNode.getTemplateNode());
    }

    @Test
    public void testHasEndOfStream() {
        when(templateNode.hasEndOfStream()).thenReturn(true).thenReturn(false);
        assertTrue(templateInstanceNode.hasEndOfStream());
        assertFalse(templateInstanceNode.hasEndOfStream());
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        templateInstanceNode.accept(mock);
        verify(mock).visit(templateInstanceNode);
        verifyNoMoreInteractions(mock);
    }

    @Test
    public void testResidentTemplate() throws IOException {
        super.setup();
        testBinaryReaderBuilder.put(unknown);
        testBinaryReaderBuilder.putDWord(templateId);
        testBinaryReaderBuilder.putDWord(5);

        BinaryReader binaryReader = testBinaryReaderBuilder.build();
        TemplateNode templateNode = mock(TemplateNode.class);
        when(templateNode.getTemplateId()).thenReturn(UnsignedInteger.valueOf(templateId));
        when(chunkHeader.addTemplateNode(5, binaryReader)).thenAnswer(invocation -> {
            binaryReader.skip(templateLength);
            return templateNode;
        });
        templateInstanceNode = new TemplateInstanceNode(binaryReader, chunkHeader, parent);
        assertEquals(templateLength, templateInstanceNode.getHeaderLength() - 10);
    }

    @Override
    protected byte getToken() {
        return BxmlNode.TEMPLATE_INSTANCE_TOKEN;
    }
}
