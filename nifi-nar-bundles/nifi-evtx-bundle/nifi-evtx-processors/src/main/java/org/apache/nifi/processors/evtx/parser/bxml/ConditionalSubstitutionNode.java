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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Template node marked for substitution with the value the root node has at the given index
 */
public class ConditionalSubstitutionNode extends BxmlNodeWithToken {
    private final int index;
    private final int type;

    public ConditionalSubstitutionNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        index = binaryReader.readWord();
        type = binaryReader.read();
        if (getFlags() != 0) {
            throw new IOException("Invalid flags");
        }
        if (getToken() != CONDITIONAL_SUBSTITUTION_TOKEN) {
            throw new IOException("Invalid token");
        }
        init();
    }

    @Override
    protected List<BxmlNode> initChildren() throws IOException {
        return Collections.emptyList();
    }

    public int getIndex() {
        return index;
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
