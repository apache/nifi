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

import org.apache.nifi.processor.io.StreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Channel Stream Callback wraps Java IO Streams in Java NIO Channels with methods for customization of channels
 */
public class ChannelStreamCallback implements StreamCallback {
    private static final int END_OF_FILE = -1;

    private final int bufferCapacity;

    /**
     * Channel Stream Callback with configurable buffer capacity for transferring bytes
     *
     * @param bufferCapacity Buffer capacity in bytes for transferring between channels
     */
    public ChannelStreamCallback(final int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    /**
     * Process streams using NIO Channels with configured Buffer
     *
     * @param inputStream Input Stream to be wrapped using a Readable Byte Channel
     * @param outputStream Output Stream to be wrapped using a Writable Byte Channel
     * @throws IOException Thrown on read or write failures
     */
    @Override
    public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        try (
                final ReadableByteChannel readableByteChannel = getReadableChannel(inputStream);
                final WritableByteChannel outputChannel = getWritableChannel(outputStream)
        ) {
            final ByteBuffer buffer = ByteBuffer.allocate(bufferCapacity);
            while (readableByteChannel.read(buffer) != END_OF_FILE) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    outputChannel.write(buffer);
                }
                buffer.clear();
            }

            buffer.flip();
            if (buffer.hasRemaining()) {
                while (buffer.hasRemaining()) {
                    outputChannel.write(buffer);
                }
            }
        }
    }

    /**
     * Get Readable Byte Channel defaults to using Channels.newChannel()
     *
     * @param inputStream Input Stream to be wrapped
     * @return Readable Byte Channel wrapping provided Input Stream
     * @throws IOException Throw on channel initialization failures
     */
    protected ReadableByteChannel getReadableChannel(final InputStream inputStream) throws IOException {
        return Channels.newChannel(inputStream);
    }

    /**
     * Get Writable Byte Channel defaults to using Channels.newChannel()
     *
     * @param outputStream Output Stream to be wrapped
     * @return Writable Byte Channel wrapping provided Output Stream
     * @throws IOException Thrown on channel initialization failures
     */
    protected WritableByteChannel getWritableChannel(final OutputStream outputStream) throws IOException {
        return Channels.newChannel(outputStream);
    }
}
