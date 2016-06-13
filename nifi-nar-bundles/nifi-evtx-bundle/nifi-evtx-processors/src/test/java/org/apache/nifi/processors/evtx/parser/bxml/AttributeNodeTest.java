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
import org.apache.nifi.processors.evtx.parser.bxml.value.NullTypeNode;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AttributeNodeTest extends BxmlNodeWithTokenAndStringTestBase {
    public static final String ATTRIBUTE_NAME = "AttributeName";
    private BinaryReader binaryReader;
    private AttributeNode attributeNode;

    @Override
    public void setup() throws IOException {
        super.setup();
        testBinaryReaderBuilder.put((byte) BxmlNode.VALUE_TOKEN);
        testBinaryReaderBuilder.put((byte) 0);
        attributeNode = new AttributeNode(testBinaryReaderBuilder.build(), chunkHeader, parent);
    }

    @Override
    protected byte getToken() {
        return BxmlNode.ATTRIBUTE_TOKEN;
    }

    @Override
    protected String getString() {
        return ATTRIBUTE_NAME;
    }

    @Test
    public void testInit() {
        assertEquals(ATTRIBUTE_NAME, attributeNode.getAttributeName());
        BxmlNode attributeNodeValue = attributeNode.getValue();
        assertTrue(attributeNodeValue instanceof ValueNode);
        List<BxmlNode> children = ((ValueNode) attributeNodeValue).getChildren();
        assertEquals(1, children.size());
        assertTrue(children.get(0) instanceof NullTypeNode);
    }

    @Test
    public void testVisit() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        attributeNode.accept(mock);
        verify(mock).visit(attributeNode);
        verifyNoMoreInteractions(mock);
    }
}
