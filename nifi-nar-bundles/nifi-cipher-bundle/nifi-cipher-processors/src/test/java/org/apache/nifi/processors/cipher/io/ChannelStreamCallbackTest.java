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
package org.apache.nifi.processors.cipher.io;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ChannelStreamCallbackTest {
    private static final int BUFFER_CAPACITY = 8192;

    private static final int BUFFER_CAPACITY_DOUBLED = 16384;

    private static final byte BYTE = 7;

    private static final int END_OF_FILE = -1;

    @Test
    void testProcessEmpty() throws IOException {
        final byte[] expected = new byte[]{};

        assertProcessSuccess(expected);
    }

    @Test
    void testProcessSingleByte() throws IOException {
        final byte[] expected = new byte[]{BYTE};

        assertProcessSuccess(expected);
    }

    @Test
    void testProcessBufferCapacity() throws IOException {
        final byte[] expected = new byte[BUFFER_CAPACITY];
        Arrays.fill(expected, BYTE);

        assertProcessSuccess(expected);
    }

    @Test
    void testProcessBufferCapacityDoubled() throws IOException {
        final byte[] expected = new byte[BUFFER_CAPACITY_DOUBLED];
        Arrays.fill(expected, BYTE);

        assertProcessSuccess(expected);
    }

    @Test
    void testProcessReadBufferRemaining() throws IOException {
        final ChannelStreamCallback callback = new BufferRemainingChannelStreamCallback();

        final byte[] expected = new byte[]{BYTE};
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(expected);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        callback.process(inputStream, outputStream);

        assertArrayEquals(expected, outputStream.toByteArray());
    }

    private void assertProcessSuccess(final byte[] expected) throws IOException {
        final ChannelStreamCallback callback = new ChannelStreamCallback(BUFFER_CAPACITY);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(expected);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        callback.process(inputStream, outputStream);

        assertArrayEquals(expected, outputStream.toByteArray());
    }

    private static class BufferRemainingChannelStreamCallback extends ChannelStreamCallback {

        public BufferRemainingChannelStreamCallback() {
            super(BUFFER_CAPACITY);
        }

        protected ReadableByteChannel getReadableChannel(final InputStream inputStream) {
            return new BufferRemainingReadableByteChannel();
        }
    }

    private static class BufferRemainingReadableByteChannel implements ReadableByteChannel {

        @Override
        public int read(final ByteBuffer buffer) {
            buffer.put(BYTE);
            return END_OF_FILE;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {

        }
    }
}
