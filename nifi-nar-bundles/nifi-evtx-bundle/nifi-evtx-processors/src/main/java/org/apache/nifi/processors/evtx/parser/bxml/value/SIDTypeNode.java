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

package org.apache.nifi.processors.evtx.parser.bxml.value;

import com.google.common.primitives.UnsignedInteger;
import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;

import java.io.IOException;

/**
 * Node containing an SID
 */
public class SIDTypeNode extends VariantTypeNode {
    private final String value;

    public SIDTypeNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent, int length) throws IOException {
        super(binaryReader, chunkHeader, parent, length);
        int version = binaryReader.read();
        int num_elements = binaryReader.read();
        UnsignedInteger id_high = binaryReader.readDWordBE();
        int id_low = binaryReader.readWordBE();
        StringBuilder builder = new StringBuilder("S-");
        builder.append(version);
        builder.append("-");
        builder.append((id_high.longValue() << 16) ^ id_low);
        for (int i = 0; i < num_elements; i++) {
            builder.append("-");
            builder.append(binaryReader.readDWord());
        }
        value = builder.toString();
    }

    @Override
    public String getValue() {
        return value;
    }
}
