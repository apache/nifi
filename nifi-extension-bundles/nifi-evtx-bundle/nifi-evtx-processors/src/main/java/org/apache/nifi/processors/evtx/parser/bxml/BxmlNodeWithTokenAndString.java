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
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.NumberUtil;

import java.io.IOException;

/**
 * Common supertype for nodes that have a token first and then a NameStringNode reference
 */
public abstract class BxmlNodeWithTokenAndString extends BxmlNodeWithToken {
    private final int stringOffset;
    private final String value;
    private final int tagLength;

    public BxmlNodeWithTokenAndString(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        stringOffset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid string offset.");
        int tagLength = getBaseTagLength();
        if (stringOffset > getOffset() - chunkHeader.getOffset()) {
            int initialPosition = binaryReader.getPosition();
            NameStringNode nameStringNode = chunkHeader.addNameStringNode(stringOffset, binaryReader);
            tagLength += binaryReader.getPosition() - initialPosition;
            value = nameStringNode.getString();
        } else {
            value = chunkHeader.getString(stringOffset);
        }
        this.tagLength = tagLength;
    }

    @Override
    protected int getHeaderLength() {
        return tagLength;
    }

    protected int getBaseTagLength() {
        return 5;
    }

    public int getStringOffset() {
        return stringOffset;
    }

    public String getStringValue() {
        return value;
    }
}
