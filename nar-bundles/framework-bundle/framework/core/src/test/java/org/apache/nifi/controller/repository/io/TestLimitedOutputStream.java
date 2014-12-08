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
package org.apache.nifi.controller.repository.io;

import org.apache.nifi.controller.repository.io.LimitedInputStream;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.io.ByteArrayInputStream;

import org.junit.Test;

public class TestLimitedOutputStream {

    @Test
    public void testSingleByteRead() throws IOException {
        final byte[] data = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6};
        final InputStream bais = new ByteArrayInputStream(data);

        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        assertEquals(0, lis.read());
        assertEquals(1, lis.read());
        assertEquals(2, lis.read());
        assertEquals(3, lis.read());
        assertEquals(-1, lis.read());
    }

    @Test
    public void testByteArrayRead() throws IOException {
        final byte[] data = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6};
        final InputStream bais = new ByteArrayInputStream(data);

        final byte[] buffer = new byte[8];

        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        final int len = lis.read(buffer);
        assertEquals(4, len);
        assertEquals(-1, lis.read(buffer));

        for (int i = 0; i < 4; i++) {
            assertEquals(i, buffer[i]);
        }
    }

    @Test
    public void testByteArrayReadWithRange() throws IOException {
        final byte[] data = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6};
        final InputStream bais = new ByteArrayInputStream(data);

        final byte[] buffer = new byte[12];

        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        final int len = lis.read(buffer, 4, 12);
        assertEquals(4, len);
        assertEquals(-1, lis.read(buffer));

        for (int i = 0; i < 4; i++) {
            assertEquals(i, buffer[i + 4]);
        }
    }
}
