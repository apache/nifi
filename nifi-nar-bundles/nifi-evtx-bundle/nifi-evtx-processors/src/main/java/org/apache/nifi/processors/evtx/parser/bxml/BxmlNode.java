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
import org.apache.nifi.processors.evtx.parser.Block;
import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Base class that helps initialization by creating children, skipping bytes where necessary
 */
public abstract class BxmlNode extends Block {
    public static final int END_OF_STREAM_TOKEN = 0x00;
    public static final int OPEN_START_ELEMENT_TOKEN = 0x01;
    public static final int CLOSE_START_ELEMENT_TOKEN = 0x02;
    public static final int CLOSE_EMPTY_ELEMENT_TOKEN = 0x03;
    public static final int CLOSE_ELEMENT_TOKEN = 0x04;
    public static final int VALUE_TOKEN = 0x05;
    public static final int ATTRIBUTE_TOKEN = 0x06;
    public static final int C_DATA_SECTION_TOKEN = 0x07;
    public static final int ENTITY_REFERENCE_TOKEN = 0x08;
    public static final int PROCESSING_INSTRUCTION_TARGET_TOKEN = 0x0A;
    public static final int PROCESSING_INSTRUCTION_DATA_TOKEN = 0x0B;
    public static final int TEMPLATE_INSTANCE_TOKEN = 0x0C;
    public static final int NORMAL_SUBSTITUTION_TOKEN = 0x0D;
    public static final int CONDITIONAL_SUBSTITUTION_TOKEN = 0x0E;
    public static final int START_OF_STREAM_TOKEN = 0x0F;

    private static final BxmlNodeFactory[] factories = new BxmlNodeFactory[]{EndOfStreamNode::new, OpenStartElementNode::new,
            CloseStartElementNode::new, CloseEmptyElementNode::new, CloseElementNode::new, ValueNode::new, AttributeNode::new,
            CDataSectionNode::new, null, EntityReferenceNode::new, ProcessingInstructionTargetNode::new,
            ProcessingInstructionDataNode::new, TemplateInstanceNode::new, NormalSubstitutionNode::new,
            ConditionalSubstitutionNode::new, StreamStartNode::new};
    private final ChunkHeader chunkHeader;
    private final BxmlNode parent;
    protected List<BxmlNode> children;
    private boolean hasEndOfStream = false;

    protected BxmlNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) {
        super(binaryReader, chunkHeader.getOffset() + binaryReader.getPosition());
        this.chunkHeader = chunkHeader;
        this.parent = parent;
        hasEndOfStream = false;
    }

    @Override
    protected void init(boolean shouldClearBinaryReader) throws IOException {
        super.init(false);
        children = Collections.unmodifiableList(initChildren());
        if (shouldClearBinaryReader) {
            clearBinaryReader();
        }
    }

    protected List<BxmlNode> initChildren() throws IOException {
        BinaryReader binaryReader = getBinaryReader();
        List<BxmlNode> result = new ArrayList<>();
        int maxChildren = getMaxChildren();
        int[] endTokens = getEndTokens();
        for (int i = 0; i < maxChildren; i++) {
            // Masking flags for location of factory
            int token = binaryReader.peek();
            int factoryIndex = token & 0x0F;
            if (factoryIndex > factories.length - 1) {
                throw new IOException("Invalid token " + factoryIndex);
            }
            BxmlNodeFactory factory = factories[factoryIndex];
            if (factory == null) {
                throw new IOException("Invalid token " + factoryIndex);
            }
            BxmlNode bxmlNode = factory.create(binaryReader, chunkHeader, this);
            result.add(bxmlNode);
            if (bxmlNode.hasEndOfStream() || bxmlNode instanceof EndOfStreamNode) {
                hasEndOfStream = true;
                break;
            }
            if (Arrays.binarySearch(endTokens, factoryIndex) >= 0) {
                break;
            }
        }
        return result;
    }

    protected int getMaxChildren() {
        return Integer.MAX_VALUE;
    }

    protected int[] getEndTokens() {
        return new int[]{END_OF_STREAM_TOKEN};
    }

    public List<BxmlNode> getChildren() {
        if (!isInitialized()) {
            throw new RuntimeException("Need to initialize children");
        }
        return children;
    }

    public ChunkHeader getChunkHeader() {
        return chunkHeader;
    }

    public BxmlNode getParent() {
        return parent;
    }

    public boolean hasEndOfStream() {
        return hasEndOfStream;
    }

    public abstract void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException;
}
