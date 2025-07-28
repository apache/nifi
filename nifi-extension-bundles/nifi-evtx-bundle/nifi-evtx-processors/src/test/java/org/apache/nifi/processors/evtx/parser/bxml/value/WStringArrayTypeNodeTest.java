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

import org.apache.nifi.processors.evtx.parser.bxml.BxmlNodeTestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WStringArrayTypeNodeTest extends BxmlNodeTestBase {
    @Test
    public void testWStringArrayTypeNodeLengthArg() throws IOException {
        String[] array = new String[]{"one", "two"};
        StringBuilder expected = new StringBuilder();
        for (String s : array) {
            expected.append("<string>");
            expected.append(s);
            expected.append("</string>");
        }
        String actual = new WStringArrayTypeNode(testBinaryReaderBuilder.putWString(String.join("\u0000", array)).build(), chunkHeader, parent, 14).getValue();
        assertEquals(expected.toString(), actual);
    }

    @Test
    public void testWStringArrayTypeNodeNoLengthArg() throws IOException {
        String[] array = new String[]{"one", "two"};
        StringBuilder expected = new StringBuilder();
        for (String s : array) {
            expected.append("<string>");
            expected.append(s);
            expected.append("</string>");
        }
        String actual = new WStringArrayTypeNode(testBinaryReaderBuilder.putWord(14).putWString(String.join("\u0000", array)).build(), chunkHeader, parent, -1).getValue();
        assertEquals(expected.toString(), actual);
    }
}
