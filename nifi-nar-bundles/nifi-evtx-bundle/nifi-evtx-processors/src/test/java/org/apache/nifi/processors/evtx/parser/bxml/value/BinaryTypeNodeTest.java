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

import com.google.common.base.Charsets;
import com.google.common.primitives.UnsignedInteger;
import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNodeTestBase;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class BinaryTypeNodeTest extends BxmlNodeTestBase {
    @Test
    public void testLength() throws IOException {
        String val = "Test String";
        BinaryReader binaryReader = testBinaryReaderBuilder.putDWord(val.length()).putString(val).build();
        assertEquals(Base64.getEncoder().encodeToString(val.getBytes(Charsets.US_ASCII)), new BinaryTypeNode(binaryReader, chunkHeader, parent, -1).getValue());
    }

    @Test(expected = IOException.class)
    public void testInvalidStringLength() throws IOException {
        String val = "Test String";
        BinaryReader binaryReader = testBinaryReaderBuilder.putDWord(UnsignedInteger.fromIntBits(Integer.MAX_VALUE + 1)).putString(val).build();
        assertEquals(Base64.getEncoder().encodeToString(val.getBytes(Charsets.US_ASCII)), new BinaryTypeNode(binaryReader, chunkHeader, parent, -1).getValue());
    }

    @Test
    public void testNoLength() throws IOException {
        String val = "Test String";
        BinaryReader binaryReader = testBinaryReaderBuilder.putString(val).build();
        assertEquals(Base64.getEncoder().encodeToString(val.getBytes(Charsets.US_ASCII)), new BinaryTypeNode(binaryReader, chunkHeader, parent, val.length()).getValue());
    }
}
