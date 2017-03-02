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
import java.util.Collections;
import java.util.List;

/**
 * Instance of a Template
 */
public class TemplateInstanceNode extends BxmlNodeWithToken {

    private final int unknown;
    private final UnsignedInteger templateId;
    private final int templateOffset;
    private final boolean isResident;
    private final TemplateNode templateNode;
    private final int templateLength;

    public TemplateInstanceNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        unknown = binaryReader.read();
        templateId = binaryReader.readDWord();
        templateOffset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid template offset.");
        if (templateOffset > getOffset() - chunkHeader.getOffset()) {
            isResident = true;
            int initialPosition = binaryReader.getPosition();
            templateNode = chunkHeader.addTemplateNode(templateOffset, binaryReader);
            templateLength = binaryReader.getPosition() - initialPosition;
        } else {
            isResident = false;
            templateNode = chunkHeader.getTemplateNode(templateOffset);
            templateLength = 0;
        }

        if (templateNode != null && !templateId.equals(templateNode.getTemplateId())) {
            throw new IOException("Invalid template id");
        }
        init();
    }

    @Override
    protected int getHeaderLength() {
        return 10 + templateLength;
    }

    public TemplateNode getTemplateNode() {
        return templateNode;
    }

    @Override
    protected List<BxmlNode> initChildren() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public boolean hasEndOfStream() {
        return super.hasEndOfStream() || templateNode.hasEndOfStream();
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
