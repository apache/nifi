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
package org.apache.nifi.remote.io;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;

import org.junit.Test;

public class TestCompressionInputOutputStreams {

    @Test
    public void testSimple() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final byte[] data = "Hello, World!".getBytes("UTF-8");

        final CompressionOutputStream cos = new CompressionOutputStream(baos);
        cos.write(data);
        cos.flush();
        cos.close();

        final byte[] compressedBytes = baos.toByteArray();
        final CompressionInputStream cis = new CompressionInputStream(new ByteArrayInputStream(compressedBytes));
        final byte[] decompressed = readFully(cis);

        assertTrue(Arrays.equals(data, decompressed));
    }

    @Test
    public void testDataLargerThanBuffer() throws IOException {
        final String str = "The quick brown fox jumps over the lazy dog\r\n\n\n\r";

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append(str);
        }
        final byte[] data = sb.toString().getBytes("UTF-8");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final CompressionOutputStream cos = new CompressionOutputStream(baos, 8192);
        cos.write(data);
        cos.flush();
        cos.close();

        final byte[] compressedBytes = baos.toByteArray();
        final CompressionInputStream cis = new CompressionInputStream(new ByteArrayInputStream(compressedBytes));
        final byte[] decompressed = readFully(cis);

        assertTrue(Arrays.equals(data, decompressed));
    }

    @Test
    public void testDataLargerThanBufferWhileFlushing() throws IOException {
        final String str = "The quick brown fox jumps over the lazy dog\r\n\n\n\r";
        final byte[] data = str.getBytes("UTF-8");

        final StringBuilder sb = new StringBuilder();
        final byte[] data1024;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final CompressionOutputStream cos = new CompressionOutputStream(baos, 8192);
        for (int i = 0; i < 1024; i++) {
            cos.write(data);
            cos.flush();
            sb.append(str);
        }
        cos.close();
        data1024 = sb.toString().getBytes("UTF-8");

        final byte[] compressedBytes = baos.toByteArray();
        final CompressionInputStream cis = new CompressionInputStream(new ByteArrayInputStream(compressedBytes));
        final byte[] decompressed = readFully(cis);

        assertTrue(Arrays.equals(data1024, decompressed));
    }

    @Test
    public void testSendingMultipleFilesBackToBackOnSameStream() throws IOException {
        final String str = "The quick brown fox jumps over the lazy dog\r\n\n\n\r";
        final byte[] data = str.getBytes("UTF-8");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final CompressionOutputStream cos = new CompressionOutputStream(baos, 8192);
        for (int i = 0; i < 512; i++) {
            cos.write(data);
            cos.flush();
        }
        cos.close();

        final CompressionOutputStream cos2 = new CompressionOutputStream(baos, 8192);
        for (int i = 0; i < 512; i++) {
            cos2.write(data);
            cos2.flush();
        }
        cos2.close();

        final byte[] data512;
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 512; i++) {
            sb.append(str);
        }
        data512 = sb.toString().getBytes("UTF-8");

        final byte[] compressedBytes = baos.toByteArray();
        final ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);

        final CompressionInputStream cis = new CompressionInputStream(bais);
        final byte[] decompressed = readFully(cis);
        assertTrue(Arrays.equals(data512, decompressed));

        final CompressionInputStream cis2 = new CompressionInputStream(bais);
        final byte[] decompressed2 = readFully(cis2);
        assertTrue(Arrays.equals(data512, decompressed2));
    }

    private byte[] readFully(final InputStream in) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final byte[] buffer = new byte[65536];
        int len;
        while ((len = in.read(buffer)) >= 0) {
            baos.write(buffer, 0, len);
        }

        return baos.toByteArray();
    }
}
