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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The strings are stored separately to conserve space
 */
public class NameStringNode extends BxmlNode {
    private final UnsignedInteger nextOffset;
    private final int hash;
    private final String string;
    private final int stringLength;

    public NameStringNode(BinaryReader binaryReader, ChunkHeader chunkHeader) throws IOException {
        super(binaryReader, chunkHeader, null);
        nextOffset = binaryReader.readDWord();
        hash = binaryReader.readWord();
        stringLength = binaryReader.readWord();
        string = binaryReader.readWString(stringLength);
        binaryReader.skip(2);
        init();
    }

    public UnsignedInteger getNextOffset() {
        return nextOffset;
    }

    public int getHash() {
        return hash;
    }

    public String getString() {
        return string;
    }

    @Override
    protected List<BxmlNode> initChildren() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }
}
