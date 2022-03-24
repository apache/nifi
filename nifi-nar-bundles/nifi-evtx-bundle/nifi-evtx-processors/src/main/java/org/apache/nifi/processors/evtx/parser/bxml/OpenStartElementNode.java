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
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.NumberUtil;

import java.io.IOException;

/**
 * Open tag in the template xml
 */
public class OpenStartElementNode extends BxmlNodeWithToken {
    private final int unknown;
    private final UnsignedInteger size;
    private final int stringOffset;
    private final String tagName;
    private final int tagLength;

    public OpenStartElementNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        if ((getFlags() & 0x0b) != 0) {
            throw new IOException("Invalid flag detected");
        }
        unknown = binaryReader.readWord();
        size = binaryReader.readDWord();
        stringOffset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid string offset.");
        int tagLength = 11;
        if ((getFlags() & 0x04) > 0) {
            tagLength += 4;
        }
        String string = getChunkHeader().getString(stringOffset);
        if (stringOffset > getOffset() - chunkHeader.getOffset()) {
            int initialPosition = binaryReader.getPosition();
            NameStringNode nameStringNode = chunkHeader.addNameStringNode(stringOffset, binaryReader);
            tagLength += binaryReader.getPosition() - initialPosition;
            tagName = nameStringNode.getString();
        } else {
            tagName = string;
        }
        this.tagLength = tagLength;
        init();
    }

    public String getTagName() {
        return tagName;
    }

    @Override
    protected int getHeaderLength() {
        return tagLength;
    }

    @Override
    protected int[] getEndTokens() {
        return new int[]{CLOSE_EMPTY_ELEMENT_TOKEN, CLOSE_ELEMENT_TOKEN};
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
