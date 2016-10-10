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

public class EntityReferenceNodeTest extends BxmlNodeWithTokenAndStringTestBase {
    public static final String AMP = "amp";
    private EntityReferenceNode entityReferenceNode;

    @Override
    public void setup() throws IOException {
        super.setup();
        entityReferenceNode = new EntityReferenceNode(testBinaryReaderBuilder.build(), chunkHeader, parent);
    }

    @Override
    protected String getString() {
        return AMP;
    }

    @Override
    protected byte getToken() {
        return BxmlNode.ENTITY_REFERENCE_TOKEN;
    }

    @Test
    public void testInit() {
        assertEquals(BxmlNode.ENTITY_REFERENCE_TOKEN, entityReferenceNode.getToken());
    }

    @Test
    public void testGetValue() {
        assertEquals("&" + AMP + ";", entityReferenceNode.getValue());
    }

    @Test
    public void testVisitor() throws IOException {
        BxmlNodeVisitor mock = mock(BxmlNodeVisitor.class);
        entityReferenceNode.accept(mock);
        verify(mock).visit(entityReferenceNode);
        verifyNoMoreInteractions(mock);
    }
}
