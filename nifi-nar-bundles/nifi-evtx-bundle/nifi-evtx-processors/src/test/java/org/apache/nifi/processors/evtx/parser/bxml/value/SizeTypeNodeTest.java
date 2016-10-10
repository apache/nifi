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
import com.google.common.primitives.UnsignedLong;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNodeTestBase;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SizeTypeNodeTest extends BxmlNodeTestBase {
    @Test
    public void testSizeTypeNodeDWord() throws IOException {
        UnsignedInteger value = UnsignedInteger.fromIntBits(Integer.MAX_VALUE + 132);
        assertEquals(value.toString(),
                new SizeTypeNode(testBinaryReaderBuilder.putDWord(value).build(), chunkHeader, parent, 4).getValue());
    }

    @Test
    public void testSizeTypeNodeQWord() throws IOException {
        UnsignedLong value = UnsignedLong.fromLongBits(Long.MAX_VALUE + 132);
        assertEquals(value.toString(),
                new SizeTypeNode(testBinaryReaderBuilder.putQWord(value).build(), chunkHeader, parent, -1).getValue());
    }
}
