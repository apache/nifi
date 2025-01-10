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
package org.apache.nifi.web.api.streaming;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteRangeStreamingOutputTest {

    private static final byte[] INPUT_BYTES = String.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

    private static final long NOT_SATISFIABLE_LENGTH = 1000;

    @Test
    void testWriteRangeZeroToUnspecified() throws IOException {
        final ByteRange byteRange = new ByteRange(0L, null);

        final byte[] outputBytes = writeBytes(byteRange);

        assertArrayEquals(INPUT_BYTES, outputBytes);
    }

    @Test
    void testWriteRangeZeroToOne() throws IOException {
        final ByteRange byteRange = new ByteRange(0L, 1L);

        final byte[] outputBytes = writeBytes(byteRange);

        assertEquals(1, outputBytes.length);

        final byte first = outputBytes[0];
        assertEquals(INPUT_BYTES[0], first);
    }

    @Test
    void testWriteRangeZeroToAvailableLength() throws IOException {
        final ByteRange byteRange = new ByteRange(0L, (long) INPUT_BYTES.length);

        final byte[] outputBytes = writeBytes(byteRange);

        assertArrayEquals(INPUT_BYTES, outputBytes);
    }

    @Test
    void testWriteRangeZeroToMaximumLong() throws IOException {
        final ByteRange byteRange = new ByteRange(0L, Long.MAX_VALUE);

        final byte[] outputBytes = writeBytes(byteRange);

        assertArrayEquals(INPUT_BYTES, outputBytes);
    }

    @Test
    void testWriteRangeOneToTwo() throws IOException {
        final ByteRange byteRange = new ByteRange(1L, 2L);

        final byte[] outputBytes = writeBytes(byteRange);

        assertEquals(1, outputBytes.length);

        final byte first = outputBytes[0];
        assertEquals(INPUT_BYTES[1], first);
    }

    @Test
    void testWriteRangeFirstPositionNotSatisfiable() {
        final ByteRange byteRange = new ByteRange(NOT_SATISFIABLE_LENGTH, Long.MAX_VALUE);

        assertThrows(RangeNotSatisfiableException.class, () -> writeBytes(byteRange));
    }

    @Test
    void testWriteRangeUnspecifiedToOne() throws IOException {
        final ByteRange byteRange = new ByteRange(null, 1L);

        final byte[] outputBytes = writeBytes(byteRange);

        assertEquals(1, outputBytes.length);

        final byte first = outputBytes[0];
        final int lastIndex = INPUT_BYTES.length - 1;
        final byte lastInput = INPUT_BYTES[lastIndex];
        assertEquals(lastInput, first);
    }

    private byte[] writeBytes(final ByteRange byteRange) throws IOException {
        final InputStream inputStream = new ByteArrayInputStream(INPUT_BYTES);

        final ByteRangeStreamingOutput streamingOutput =  new ByteRangeStreamingOutput(inputStream, byteRange);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);

        return outputStream.toByteArray();
    }
}
