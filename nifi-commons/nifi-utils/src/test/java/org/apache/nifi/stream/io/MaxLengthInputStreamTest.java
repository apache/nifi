/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.stream.io;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MaxLengthInputStreamTest {

    @Test
    public void testReadAllBytesReadingLessThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, Integer.MAX_VALUE)) {
            final byte[] allBytes = maxLengthInputStream.readAllBytes();
            assertEquals(5, allBytes.length);
        }
    }

    @Test
    public void testReadAllBytesReadingMoreThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 1)) {
            assertThrows(IOException.class, maxLengthInputStream::readAllBytes);
        }
    }

    @Test
    public void testReadAllBytesReadingEqualToMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 5)) {
            final byte[] allBytes = maxLengthInputStream.readAllBytes();
            assertEquals(5, allBytes.length);
        }
    }

    @Test
    public void testReadBufferReadingLessThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, Integer.MAX_VALUE)) {
            final byte[] buf = new byte[5];
            assertEquals(5, maxLengthInputStream.read(buf));
            assertEquals(content, new String(buf, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testReadBufferReadingMoreThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 1)) {
            final byte[] buf = new byte[5];
            assertThrows(IOException.class, () -> maxLengthInputStream.read(buf));
        }
    }

    @Test
    public void testReadBufferReadingEqualToMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 5)) {
            final byte[] buf = new byte[5];
            assertEquals(5, maxLengthInputStream.read(buf));
            assertEquals(content, new String(buf, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testTransferToLessThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, Integer.MAX_VALUE);
             final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            maxLengthInputStream.transferTo(outputStream);
            assertEquals(content, outputStream.toString(StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testTransferToMoreThanMaxLength() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 3);
             final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            assertThrows(IOException.class, () -> maxLengthInputStream.transferTo(outputStream));
        }
    }

    @Test
    public void testMarkResetSkipAvailable() throws IOException {
        final String content = "12345";
        try (final InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
             final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, 3)) {
            maxLengthInputStream.mark(10);
            assertEquals(5, maxLengthInputStream.available());
            assertEquals(4, maxLengthInputStream.skip(4));
            assertEquals(1, maxLengthInputStream.available());
            assertThrows(IOException.class, maxLengthInputStream::read);

            maxLengthInputStream.reset();
            final byte[] buf = new byte[3];
            assertEquals(3, maxLengthInputStream.read(buf));
            assertEquals(content.substring(0, 3), new String(buf, StandardCharsets.UTF_8));
        }
    }
}
