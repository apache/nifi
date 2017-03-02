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
 * Template node describing structure of xml to be rendered into
 */
public class TemplateNode extends BxmlNode {
    private final int nextOffset;
    private final UnsignedInteger templateId;
    private final String guid;
    private final int dataLength;

    public TemplateNode(BinaryReader binaryReader, ChunkHeader chunkHeader) throws IOException {
        super(binaryReader, chunkHeader, null);
        nextOffset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid offset.");

        //TemplateId and Guid overlap
        templateId = new BinaryReader(binaryReader, binaryReader.getPosition()).readDWord();
        guid = binaryReader.readGuid();
        dataLength = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE - 0x18, "Data length too large.");
        init();
    }

    @Override
    public String toString() {
        return "TemplateNode{" +
                "nextOffset=" + nextOffset +
                ", templateId=" + templateId +
                ", guid='" + guid + '\'' +
                ", dataLength=" + dataLength +
                '}';
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public UnsignedInteger getTemplateId() {
        return templateId;
    }

    public String getGuid() {
        return guid;
    }

    public int getDataLength() {
        return dataLength;
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
