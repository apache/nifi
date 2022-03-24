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
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.bxml.value.BXmlTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.BinaryTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.BooleanTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.DoubleTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.FiletimeTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.FloatTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.GuidTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.Hex32TypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.Hex64TypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.NullTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SIDTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SignedByteTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SignedDWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SignedQWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SignedWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SizeTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.StringTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.SystemtimeTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.UnsignedByteTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.UnsignedDWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.UnsignedQWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.UnsignedWordTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.VariantTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.VariantTypeNodeFactory;
import org.apache.nifi.processors.evtx.parser.bxml.value.WStringArrayTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.WStringTypeNode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Node type that has one VariantTypeNode child
 */
public class ValueNode extends BxmlNodeWithToken {
    public static final Map<Integer, VariantTypeNodeFactory> factories = initFactories();
    private final int type;

    public ValueNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        if ((getFlags() & 0x0B) != 0) {
            throw new IOException("Invalid flag");
        }
        type = binaryReader.read();
        init();
    }

    private static final Map<Integer, VariantTypeNodeFactory> initFactories() {
        Map<Integer, VariantTypeNodeFactory> result = new HashMap<>();
        result.put(0, NullTypeNode::new);
        result.put(1, WStringTypeNode::new);
        result.put(2, StringTypeNode::new);
        result.put(3, SignedByteTypeNode::new);
        result.put(4, UnsignedByteTypeNode::new);
        result.put(5, SignedWordTypeNode::new);
        result.put(6, UnsignedWordTypeNode::new);
        result.put(7, SignedDWordTypeNode::new);
        result.put(8, UnsignedDWordTypeNode::new);
        result.put(9, SignedQWordTypeNode::new);
        result.put(10, UnsignedQWordTypeNode::new);
        result.put(11, FloatTypeNode::new);
        result.put(12, DoubleTypeNode::new);
        result.put(13, BooleanTypeNode::new);
        result.put(14, BinaryTypeNode::new);
        result.put(15, GuidTypeNode::new);
        result.put(16, SizeTypeNode::new);
        result.put(17, FiletimeTypeNode::new);
        result.put(18, SystemtimeTypeNode::new);
        result.put(19, SIDTypeNode::new);
        result.put(20, Hex32TypeNode::new);
        result.put(21, Hex64TypeNode::new);
        result.put(33, BXmlTypeNode::new);
        result.put(129, WStringArrayTypeNode::new);
        return Collections.unmodifiableMap(result);
    }

    @Override
    protected List<BxmlNode> initChildren() throws IOException {
        VariantTypeNode variantTypeNode = factories.get(type).create(getBinaryReader(), getChunkHeader(), this, -1);
        return Collections.singletonList(variantTypeNode);
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
