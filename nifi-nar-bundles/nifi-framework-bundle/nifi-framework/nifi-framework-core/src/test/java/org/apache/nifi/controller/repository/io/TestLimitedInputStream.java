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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.stream.io.ByteArrayInputStream;

import org.junit.Test;

public class TestLimitedInputStream {

    final byte[] data = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6};
    final InputStream bais = new ByteArrayInputStream(data);
    final byte[] buffer3 = new byte[3];
    final byte[] buffer10 = new byte[10];

    @Test
    public void testSingleByteRead() throws IOException {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        assertEquals(0, lis.read());
        assertEquals(1, lis.read());
        assertEquals(2, lis.read());
        assertEquals(3, lis.read());
        assertEquals(-1, lis.read());
    }

    @Test
    public void testByteArrayRead() throws IOException {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        final int len = lis.read(buffer10);
        assertEquals(4, len);
        assertEquals(-1, lis.read(buffer10));

        for (int i = 0; i < 4; i++) {
            assertEquals(i, buffer10[i]);
        }
    }

    @Test
    public void testByteArrayReadWithRange() throws IOException {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        final int len = lis.read(buffer10, 4, 12);
        assertEquals(4, len);
        assertEquals(-1, lis.read(buffer10, 8, 2));

        for (int i = 0; i < 4; i++) {
            assertEquals(i, buffer10[i + 4]);
        }
    }


    @Test
    public void testSkip() throws Exception {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        assertEquals(3, lis.read(buffer3));
        assertEquals(1, lis.skip(data.length));
        lis.reset();
        assertEquals(4, lis.skip(7));
        lis.reset();
        assertEquals(2, lis.skip(2));
    }

    @Test
    public void testClose() {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        try {
            lis.close();
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testAvailable() throws Exception {
        final LimitedInputStream lis = new LimitedInputStream(bais, 4);
        assertNotEquals(data.length, lis.available());
        lis.reset();
        assertEquals(4, lis.available());
        assertEquals(1, lis.read(buffer3, 0, 1));
        assertEquals(3, lis.available());
    }

    @Test
    public void testMarkSupported() {
        final LimitedInputStream lis = new LimitedInputStream(bais, 6);
        assertEquals(bais.markSupported(), lis.markSupported());
    }

    @Test
    public void testMark() throws Exception {
        final LimitedInputStream lis = new LimitedInputStream(bais, 6);
        assertEquals(3, lis.read(buffer3));
        assertEquals(3, lis.read(buffer10));
        lis.reset();
        assertEquals(3, lis.read(buffer3));
        lis.mark(1000);
        assertEquals(3, lis.read(buffer10));
        lis.reset();
        assertEquals(3, lis.read(buffer10));
    }

}
